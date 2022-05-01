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
                .expect("TxErrorPropagator must be used within a AsyncTx context")
                .signal_error(self.0.clone())
        });
        Poll::Pending
    }
}

#[macro_export]
macro_rules! abort {
    () => {{
        use $crate::transaction::error::TxErrorPropagator;
        TxErrorPropagator::new(TxError::Aborted).await;
    }};
}
