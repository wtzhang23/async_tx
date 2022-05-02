use crossbeam::deque::{Injector, Steal};
use parking_lot::{Condvar, Mutex};
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake};

struct ExecutorWaker {
    lock: Mutex<bool>,
    cv: Condvar,
}

impl ExecutorWaker {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    pub fn wait(&self) {
        self.invoke_then_wait(|| true);
    }

    pub fn invoke_then_wait<F>(&self, f: F)
    where
        F: FnOnce() -> bool,
    {
        let mut guard = self.lock.lock();
        if f() {
            while !*guard {
                self.cv.wait(&mut guard);
            }
            *guard = false;
        }
    }

    pub fn wake(&self) {
        self.invoke_then_wake(|| true);
    }

    pub fn invoke_then_wake<F>(&self, f: F)
    where
        F: FnOnce() -> bool,
    {
        let mut guard = self.lock.lock();
        if f() && !*guard {
            *guard = true;
            self.cv.notify_one();
        }
    }
}

impl Wake for ExecutorWaker {
    fn wake(self: Arc<Self>) {
        Self::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Self::wake(self)
    }
}

unsafe impl Send for ExecutorWaker {}
unsafe impl Sync for ExecutorWaker {}

pub struct FutureExecutor<F>
where
    F: Future,
{
    future: Pin<Box<F>>,
    waker: Arc<ExecutorWaker>,
}

impl<F> FutureExecutor<F>
where
    F: Future,
{
    pub fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
            waker: Arc::new(ExecutorWaker::new()),
        }
    }

    pub fn run(mut self) -> F::Output {
        loop {
            match self.run_till_poll() {
                Some(output) => return output,
                None => self.waker.wait(),
            }
        }
    }

    pub fn run_till_poll(&mut self) -> Option<F::Output> {
        let waker = self.waker.clone().into();
        let mut cx = Context::from_waker(&waker);
        match self.future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        }
    }
}

pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    FutureExecutor::new(future).run()
}

type Task = UnsafeCell<dyn Future<Output = ()>>;

struct LocalExecutorContext {
    injector: Injector<Pin<Arc<Task>>>,
    executor: ExecutorWaker,
}

impl LocalExecutorContext {
    fn new() -> Self {
        Self {
            injector: Injector::new(),
            executor: ExecutorWaker::new(),
        }
    }

    fn enqueue(&self, task: Pin<Arc<Task>>) {
        self.injector.push(task);
    }

    fn dequeue(&self) -> Option<Pin<Arc<Task>>> {
        loop {
            match self.injector.steal() {
                Steal::Empty => return None,
                Steal::Success(task) => return Some(task),
                Steal::Retry => {}
            }
        }
    }
}

struct LocalExecutorWaker {
    future: AtomicPtr<Pin<Arc<Task>>>,
    ready: AtomicBool,
    executor_ctx: Weak<LocalExecutorContext>,
}

impl LocalExecutorWaker {
    fn new(ctx: &Arc<LocalExecutorContext>) -> Self {
        Self {
            future: AtomicPtr::new(std::ptr::null_mut()),
            ready: AtomicBool::new(false),
            executor_ctx: Arc::downgrade(ctx),
        }
    }

    fn woken(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    fn register_future(&self, future: Pin<Arc<Task>>) {
        let boxed = Box::into_raw(Box::new(future));
        self.future.store(boxed, Ordering::Relaxed);
    }
}

impl Wake for LocalExecutorWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Relaxed);
        let upgraded = self.executor_ctx.upgrade();
        if let Some(ctx) = upgraded.as_ref() {
            ctx.executor.invoke_then_wake(|| {
                let future_ptr = self.future.swap(std::ptr::null_mut(), Ordering::AcqRel);
                if !future_ptr.is_null() {
                    let future = unsafe { *Box::from_raw(future_ptr) };
                    ctx.enqueue(future);
                    true
                } else {
                    false
                }
            });
        }
    }
}

unsafe impl Send for LocalExecutorWaker {}
unsafe impl Sync for LocalExecutorWaker {}

pub struct LocalExecutor {
    ctx: Arc<LocalExecutorContext>,
    enqueued_futures: AtomicUsize,
}

impl LocalExecutor {
    pub fn new() -> Self {
        Self {
            ctx: Arc::new(LocalExecutorContext::new()),
            enqueued_futures: AtomicUsize::new(0),
        }
    }

    pub fn num_enqueued(&self) -> usize {
        self.enqueued_futures.load(Ordering::Relaxed)
    }

    pub fn enqueue<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.ctx.enqueue(Arc::pin(UnsafeCell::new(task)));
        self.enqueued_futures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn run_until<F>(&mut self, future: F) -> F::Output
    where
        F: Future + 'static,
    {
        let mut executor = Some(FutureExecutor::new(future));
        let mut result = None;
        self.run_until_condition(|current| {
            if let Some(val) = executor.as_mut().unwrap().run_till_poll() {
                result = Some(val);
                true
            } else if current.enqueued_futures.load(Ordering::Relaxed) == 0 {
                result = Some(executor.take().unwrap().run());
                true
            } else {
                false
            }
        });
        result.unwrap()
    }

    pub fn run_all(&mut self) {
        self.run_until_condition(|current| current.enqueued_futures.load(Ordering::Relaxed) == 0);
    }
}

enum WaitStatus {
    Ready,
    DequeueSuccess(Pin<Arc<Task>>),
    DequeueFailed,
}
impl WaitStatus {
    fn need_to_block(&self) -> bool {
        matches!(self, WaitStatus::DequeueFailed)
    }
}

impl LocalExecutor {
    fn wait_for_new_task(&self, need_wait: bool) -> WaitStatus {
        if !need_wait {
            WaitStatus::Ready
        } else if let Some(task) = self.ctx.dequeue() {
            WaitStatus::DequeueSuccess(task)
        } else {
            WaitStatus::DequeueFailed
        }
    }

    fn run_until_condition<F>(&mut self, mut condition: F)
    where
        F: FnMut(&mut Self) -> bool,
    {
        if condition(self) {
            return;
        }
        if let Some(task) = self.ctx.dequeue() {
            let mut next_task = task;
            loop {
                let wake = Arc::new(LocalExecutorWaker::new(&self.ctx));
                let waker = wake.clone().into();
                let mut cx = Context::from_waker(&waker);

                let poll = {
                    let pinned_future = unsafe {
                        Pin::new_unchecked(next_task.as_ref().get().as_mut().unwrap_unchecked())
                    }; // ptr cannot be null since next_task is not null
                    pinned_future.poll(&mut cx)
                };
                {
                    let cur_task = next_task;
                    let mut wait_status = None;

                    if matches!(&poll, Poll::Ready(_)) {
                        let prev = self.enqueued_futures.fetch_sub(1, Ordering::Relaxed);
                        debug_assert!(prev >= 1);
                    }
                    let ready = condition(self);

                    match poll {
                        Poll::Ready(()) => {
                            self.ctx.executor.invoke_then_wait(|| {
                                let ws = self.wait_for_new_task(!ready);
                                let need_to_block = ws.need_to_block();
                                wait_status = Some(ws);
                                need_to_block
                            });
                        }
                        Poll::Pending => {
                            self.ctx.executor.invoke_then_wait(|| {
                                let ws = {
                                    if !wake.woken() {
                                        wake.register_future(cur_task);
                                        self.wait_for_new_task(!ready)
                                    } else {
                                        self.ctx.enqueue(cur_task);
                                        if ready {
                                            WaitStatus::Ready
                                        } else {
                                            WaitStatus::DequeueSuccess(self.ctx.dequeue().unwrap())
                                        }
                                    }
                                };
                                let need_to_block = ws.need_to_block();
                                wait_status = Some(ws);
                                need_to_block
                            });
                        }
                    };

                    debug_assert_eq!(
                        ready,
                        matches!(wait_status.as_ref().unwrap(), WaitStatus::Ready)
                    );
                    loop {
                        debug_assert!(wait_status.is_some());
                        match unsafe { wait_status.unwrap_unchecked() } {
                            WaitStatus::Ready => {
                                return;
                            }
                            WaitStatus::DequeueSuccess(new_task) => {
                                next_task = new_task;
                                break;
                            }
                            WaitStatus::DequeueFailed => {
                                if let Some(new_task) = self.ctx.dequeue() {
                                    next_task = new_task;
                                    break;
                                } else {
                                    let ready = condition(self);
                                    let mut next_wait_status = None;
                                    self.ctx.executor.invoke_then_wait(|| {
                                        let ws = self.wait_for_new_task(!ready);
                                        let need_to_block = ws.need_to_block();
                                        next_wait_status = Some(ws);
                                        need_to_block
                                    });
                                    debug_assert!(next_wait_status.is_some());
                                    wait_status = next_wait_status;
                                    debug_assert_eq!(
                                        ready,
                                        matches!(wait_status.as_ref().unwrap(), WaitStatus::Ready)
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}
