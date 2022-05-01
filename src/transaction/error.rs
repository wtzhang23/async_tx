use crate::context::CUR_CTX;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Clone, Debug)]
pub enum TxError {
    Aborted,
}

pub struct TxErrorPropagator(TxError);

impl TxErrorPropagator {
    pub fn new(err: TxError) -> Self {
        Self(err)
    }
}

impl Future for TxErrorPropagator {
    type Output = (); // should be unreachable

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        CUR_CTX.with(|cur_ctx| {
            cur_ctx
                .borrow_mut()
                .as_mut()
                .unwrap()
                .signal_error(self.0.clone())
        });
        Poll::Pending
    }
}
