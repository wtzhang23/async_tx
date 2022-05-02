use crate::context::{in_ctx, signal_err};
use crate::{
    context::flags::TxWaitFlag, context::CUR_CTX, data::containers::TxDataContainer,
    data::TxDataHandle, transaction::error::TxError,
};
use std::{
    borrow::BorrowMut,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub trait TxWaitable {
    fn enqueue_wait(self: Box<Self>, waker: Waker, flag: TxWaitFlag);
    fn wake(self: Box<Self>);
}

pub struct TxDataWaiter {
    waiters: Option<Vec<Box<dyn TxWaitable>>>,
    wait_flag: TxWaitFlag,
}

impl TxDataWaiter {
    pub fn new() -> Self {
        Self {
            waiters: Some(Vec::new()),
            wait_flag: TxWaitFlag::new(),
        }
    }

    pub async fn add_handle<C, R>(&mut self, mut handle: R)
    where
        C: TxDataContainer + Send + Sync + 'static,
        <C as TxDataContainer>::DataType: Send + Sync,
        R: BorrowMut<TxDataHandle<C>>,
    {
        handle.borrow_mut().read().await;
        let handle = handle.borrow().wait_handle();
        self.add_waiter(handle);
    }

    fn add_waiter<T: TxWaitable + 'static>(&mut self, waitable: T) {
        self.waiters
            .as_mut()
            .expect("Cannot add waiter after already used as a future")
            .push(Box::new(waitable));
    }
}

impl Default for TxDataWaiter {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for TxDataWaiter {
    type Output = (); // should never unblock

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(waiters) = self.waiters.take() {
            if waiters.is_empty() {
                return Poll::Ready(());
            }

            waiters
                .into_iter()
                .for_each(|waiter| waiter.enqueue_wait(cx.waker().clone(), self.wait_flag.clone()));

            CUR_CTX.with(|ctx| {
                if let Some(cur_ctx) = ctx.borrow_mut().as_mut() {
                    cur_ctx.unlock_all_locks();
                }
            });
        }

        if self.wait_flag.awake() && in_ctx() {
            unsafe { signal_err(TxError::Aborted) }
        }
        Poll::Pending
    }
}

#[macro_export]
macro_rules! wait {
    ($($x: expr),*) => {
        {
            use $crate::transaction::condvar::TxDataWaiter;
            let mut waiter = TxDataWaiter::new();
            $(
                waiter.add_handle(&mut $x).await;
            ),*
            waiter.await;
            unreachable!();
        }
    };
}
