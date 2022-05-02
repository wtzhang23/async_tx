mod channel;

use crossbeam::deque::{Injector, Steal, Stealer, Worker};
use parking_lot::{Condvar, Mutex};
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Wake};

use self::channel::single_value;

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

type Task<F> = UnsafeCell<F>;
type PinnedTask<F> = Pin<Arc<Task<F>>>;

struct TypedLocalExecutorContext<F>
where
    F: Future<Output = ()> + ?Sized,
{
    queue: Worker<PinnedTask<F>>,
    executor: ExecutorWaker,
    outbound: AtomicUsize,
}

impl<F> TypedLocalExecutorContext<F>
where
    F: Future<Output = ()> + ?Sized,
{
    fn new() -> Self {
        Self {
            queue: Worker::new_fifo(),
            executor: ExecutorWaker::new(),
            outbound: AtomicUsize::new(0),
        }
    }

    fn enqueue_new_task(&self, task: PinnedTask<F>) {
        self.queue.push(task);
    }

    fn enqueue_old_task(&self, task: PinnedTask<F>) {
        self.queue.push(task);
        let prev = self.outbound.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0);
    }

    fn dequeue_task(&self) -> Option<PinnedTask<F>> {
        let output = self.queue.pop();
        if output.is_some() {
            self.outbound.fetch_add(1, Ordering::Relaxed);
        }
        output
    }

    fn close_task(&self) {
        let prev = self.outbound.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(prev > 0);
    }

    fn idle(&self) -> bool {
        self.queue.is_empty() && self.outbound.load(Ordering::Relaxed) == 0
    }

    fn wake(&self) {
        self.executor.wake()
    }
}

unsafe impl<F> Sync for TypedLocalExecutorContext<F> where F: Future<Output = ()> + ?Sized + Sync {}
unsafe impl<F> Send for TypedLocalExecutorContext<F> where F: Future<Output = ()> + ?Sized + Send {}

struct TypedLocalExecutorWaker<F>
where
    F: Future<Output = ()> + ?Sized,
{
    future: AtomicPtr<PinnedTask<F>>,
    ready: AtomicBool,
    executor_ctx: Weak<TypedLocalExecutorContext<F>>,
}

impl<F> TypedLocalExecutorWaker<F>
where
    F: Future<Output = ()> + ?Sized,
{
    fn new(ctx: &Arc<TypedLocalExecutorContext<F>>) -> Self {
        Self {
            future: AtomicPtr::new(std::ptr::null_mut()),
            ready: AtomicBool::new(false),
            executor_ctx: Arc::downgrade(ctx),
        }
    }

    fn woken(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    fn register_future(&self, future: PinnedTask<F>) {
        let boxed = Box::into_raw(Box::new(future));
        self.future.store(boxed, Ordering::Relaxed);
    }
}

impl<F> Wake for TypedLocalExecutorWaker<F>
where
    F: Future<Output = ()> + ?Sized,
{
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
                    ctx.enqueue_old_task(future);
                    true
                } else {
                    false
                }
            });
        }
    }
}

unsafe impl<F> Send for TypedLocalExecutorWaker<F> where F: Future<Output = ()> + ?Sized {}
unsafe impl<F> Sync for TypedLocalExecutorWaker<F> where F: Future<Output = ()> + ?Sized {}

enum WaitStatus<F>
where
    F: Future<Output = ()> + ?Sized,
{
    Ready,
    DequeueSuccess(PinnedTask<F>),
    DequeueFailed,
}
impl<F> WaitStatus<F>
where
    F: Future<Output = ()> + ?Sized,
{
    fn need_to_block(&self) -> bool {
        matches!(self, WaitStatus::DequeueFailed)
    }
}

struct TypedLocalExecutor<F>
where
    F: Future<Output = ()> + ?Sized,
{
    ctx: Arc<TypedLocalExecutorContext<F>>,
}

impl<F> TypedLocalExecutor<F>
where
    F: Future<Output = ()> + ?Sized,
{
    fn new() -> Self {
        Self {
            ctx: Arc::new(TypedLocalExecutorContext::new()),
        }
    }

    fn enqueue_new_task(&self, task: PinnedTask<F>) {
        self.ctx.enqueue_new_task(task);
    }

    fn enqueue_old_task(&self, task: PinnedTask<F>) {
        self.ctx.enqueue_old_task(task);
    }

    fn context(&self) -> &Arc<TypedLocalExecutorContext<F>> {
        &self.ctx
    }

    fn idle(&self) -> bool {
        self.ctx.idle()
    }
}

impl<F> TypedLocalExecutor<F>
where
    F: Future<Output = ()> + ?Sized + 'static,
{
    fn wait_for_new_task(&self, need_wait: bool) -> WaitStatus<F> {
        if !need_wait {
            WaitStatus::Ready
        } else if let Some(task) = self.ctx.dequeue_task() {
            WaitStatus::DequeueSuccess(task)
        } else {
            WaitStatus::DequeueFailed
        }
    }

    fn run_until_condition<C>(&mut self, mut condition: C)
    where
        C: FnMut(&mut Self) -> bool,
    {
        if condition(self) {
            return;
        }
        if let Some(task) = self.ctx.dequeue_task() {
            let mut next_task = task;
            loop {
                let wake = Arc::new(TypedLocalExecutorWaker::new(&self.ctx));
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

                    if matches!(poll, Poll::Ready(_)) {
                        self.ctx.close_task();
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
                                        self.enqueue_old_task(cur_task);
                                        if ready {
                                            WaitStatus::Ready
                                        } else {
                                            WaitStatus::DequeueSuccess(
                                                self.ctx.dequeue_task().unwrap(),
                                            )
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
                                if let Some(new_task) = self.ctx.dequeue_task() {
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

impl<F> Default for TypedLocalExecutor<F>
where
    F: Future<Output = ()> + ?Sized,
{
    fn default() -> Self {
        Self::new()
    }
}

type LocalFuture = dyn Future<Output = ()>;
type GlobalFuture = dyn Future<Output = ()> + Send + Sync + 'static;

#[derive(Default)]
pub struct LocalExecutor {
    typed: TypedLocalExecutor<LocalFuture>,
}

impl LocalExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.typed
            .enqueue_new_task(Arc::pin(UnsafeCell::new(future)));
    }

    pub fn run_all(&mut self) {
        self.typed.run_until_condition(|current| current.idle());
    }

    pub fn run_until<O>(&mut self, future: O) -> O::Output
    where
        O: Future + 'static,
    {
        let (tx, rx) = single_value();
        let producer = async move {
            tx.send(future.await).await;
        };
        self.enqueue(producer);

        let mut executor = Some(FutureExecutor::new(rx.recv()));
        let mut result = None;
        self.typed.run_until_condition(|current| {
            if let Some(val) = executor.as_mut().unwrap().run_till_poll() {
                result = Some(val);
                true
            } else if current.idle() {
                result = Some(executor.take().unwrap().run());
                true
            } else {
                false
            }
        });
        result.unwrap()
    }
}

#[derive(Default)]
struct GlobalLocalExecutor {
    typed: TypedLocalExecutor<GlobalFuture>,
}

impl GlobalLocalExecutor {
    fn new() -> Self {
        Self::default()
    }

    fn run_one(&mut self) {
        let mut ran = 0;
        self.typed.run_until_condition(|_exeutor| {
            ran += 1;
            ran == 2
        });
    }

    fn context(&self) -> &Arc<TypedLocalExecutorContext<GlobalFuture>> {
        self.typed.context()
    }
}

struct GlobalExecutorStealer {
    ctx: Arc<TypedLocalExecutorContext<GlobalFuture>>,
    stealer: Stealer<PinnedTask<GlobalFuture>>,
}

impl GlobalExecutorStealer {
    fn new(ctx: &Arc<TypedLocalExecutorContext<GlobalFuture>>) -> Self {
        Self {
            ctx: ctx.clone(),
            stealer: ctx.queue.stealer(),
        }
    }

    fn wake(&self) {
        self.ctx.wake()
    }
}

struct GlobalExecutorInner {
    global_queue: Injector<PinnedTask<GlobalFuture>>,
    local_queues: Vec<GlobalExecutorStealer>,
}

impl GlobalExecutorInner {
    fn new(num_threads: usize) -> Self {
        let global_queue = Injector::new();
        let local_queues = Vec::with_capacity(num_threads);
        Self {
            global_queue,
            local_queues,
        }
    }

    fn add_local_executor(&mut self, local_executor: &GlobalLocalExecutor) {
        self.local_queues
            .push(GlobalExecutorStealer::new(local_executor.context()));
    }

    fn enqueue_new_task(&self, task: PinnedTask<GlobalFuture>) {
        self.global_queue.push(task);
        self.local_queues.iter().for_each(|queue| queue.wake());
    }

    fn steal_one(&self, stealer: &TypedLocalExecutorContext<GlobalFuture>) {
        let mut retry = true;
        while retry {
            retry = false;
            match self.global_queue.steal_batch(&stealer.queue) {
                Steal::Empty => {}
                Steal::Success(()) => return,
                Steal::Retry => {
                    retry = true;
                }
            }

            for local_queue in &self.local_queues {
                match local_queue.stealer.steal() {
                    Steal::Empty => {}
                    Steal::Success(task) => {
                        stealer.enqueue_new_task(task);
                        return;
                    }
                    Steal::Retry => {
                        retry = true;
                    }
                }
            }
        }
    }

    fn try_fetch_one(&self, fetcher: &TypedLocalExecutorContext<GlobalFuture>) {
        loop {
            let taken = self.global_queue.steal();
            match taken {
                Steal::Empty => break,
                Steal::Success(val) => {
                    fetcher.enqueue_new_task(val);
                    break;
                }
                Steal::Retry => {}
            }
        }
    }
}

unsafe impl Send for GlobalExecutorInner {}
unsafe impl Sync for GlobalExecutorInner {}

pub struct GlobalExecutor {
    inner: Arc<GlobalExecutorInner>,
}

impl GlobalExecutor {
    pub fn new(num_threads: usize) -> Self {
        let mut local_queues = Vec::with_capacity(num_threads);
        for _thread_idx in 0..num_threads {
            local_queues.push(GlobalLocalExecutor::new());
        }

        let mut inner = GlobalExecutorInner::new(num_threads);
        for local_queue in &local_queues {
            inner.add_local_executor(local_queue);
        }
        let inner = Arc::new(inner);
        for (_thread_idx, mut local_executor) in local_queues.into_iter().enumerate() {
            let inner = Arc::downgrade(&inner);
            std::thread::spawn(move || {
                while let Some(inner) = inner.upgrade() {
                    if local_executor.context().idle() {
                        local_executor.context().executor.invoke_then_wait(|| {
                            inner.steal_one(local_executor.context());
                            local_executor.context().idle()
                        });
                    } else {
                        inner.try_fetch_one(local_executor.context());
                    }
                    local_executor.run_one();
                }
            });
        }

        Self { inner }
    }

    pub fn enqueue<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.inner
            .enqueue_new_task(Arc::pin(UnsafeCell::new(future)));
    }
}
