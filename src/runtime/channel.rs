use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

struct SingleValue<T> {
    data: AtomicPtr<T>,
    waker: AtomicPtr<Waker>,
}

impl<T> SingleValue<T> {
    pub fn new() -> Self {
        Self {
            data: AtomicPtr::new(std::ptr::null_mut()),
            waker: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

impl<T> Drop for SingleValue<T> {
    fn drop(&mut self) {
        let data = self.data.load(Ordering::Relaxed);
        if !data.is_null() {
            unsafe {
                Box::from_raw(data);
            }
        }

        let waker = self.waker.load(Ordering::Relaxed);
        if !waker.is_null() {
            unsafe {
                Box::from_raw(waker);
            }
        }
    }
}

unsafe impl<T> Send for SingleValue<T> where T: Send {}

unsafe impl<T> Sync for SingleValue<T> where T: Sync {}

pub struct SingleValueTx<T> {
    value: Arc<SingleValue<T>>,
}

impl<T> SingleValueTx<T> {
    pub async fn send(self, val: T) {
        let boxed = Box::into_raw(Box::new(val));
        self.value.data.store(boxed, Ordering::Release);
        let waker = self.value.waker.load(Ordering::Acquire);
        if !waker.is_null() {
            unsafe {
                let waker = Box::from_raw(waker);
                waker.wake();
            }
        }
    }
}

pub struct SingleValueRx<T> {
    value: Arc<SingleValue<T>>,
}

impl<T> SingleValueRx<T> {
    pub async fn recv(self) -> T {
        self.await
    }
}

impl<T> Future for SingleValueRx<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let boxed = Box::into_raw(Box::new(cx.waker().clone()));
        self.value.waker.store(boxed, Ordering::Release);

        let data = self
            .value
            .data
            .swap(std::ptr::null_mut(), Ordering::Acquire);
        if !data.is_null() {
            unsafe { Poll::Ready(*Box::from_raw(data)) }
        } else {
            Poll::Pending
        }
    }
}

pub fn single_value<T>() -> (SingleValueTx<T>, SingleValueRx<T>) {
    let sv = Arc::new(SingleValue::new());
    let tx = SingleValueTx { value: sv.clone() };
    let rx = SingleValueRx { value: sv };
    (tx, rx)
}
