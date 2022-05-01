use parking_lot::{Condvar, Mutex};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake};

pub struct SingleFutureExecutor<F>
where
    F: Future,
{
    future: Pin<Box<F>>,
    waker: Arc<SingleFutureExecutorWaker>,
}

impl<F> SingleFutureExecutor<F>
where
    F: Future,
{
    pub fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
            waker: Arc::new(SingleFutureExecutorWaker::new()),
        }
    }

    pub fn execute(mut self) -> F::Output {
        loop {
            let waker = self.waker.clone().into();
            let mut cx = Context::from_waker(&waker);
            match self.future.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => self.waker.wait(),
            }
        }
    }
}

struct SingleFutureExecutorWaker {
    lock: Mutex<bool>,
    cv: Condvar,
}

impl SingleFutureExecutorWaker {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    pub fn wait(&self) {
        let mut guard = self.lock.lock();
        while !*guard {
            self.cv.wait(&mut guard);
        }
        *guard = false;
    }
}

impl Wake for SingleFutureExecutorWaker {
    fn wake(self: Arc<Self>) {
        Self::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut guard = self.lock.lock();
        if !*guard {
            *guard = true;
            self.cv.notify_one();
        }
    }
}
