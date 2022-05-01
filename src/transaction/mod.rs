pub mod condvar;
pub mod error;

use crate::context::{TxContext, CUR_CTX, CUR_LVL};
use error::TxError;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub struct AsyncTx<F>
where
    F: Future,
{
    priority: usize,
    future: F,
    started: bool,
    context: Option<TxContext>,
}

impl<F> AsyncTx<F>
where
    F: Future,
{
    pub fn with_priority(future: F, priority: usize) -> Self {
        Self {
            future,
            priority,
            started: false,
            context: None,
        }
    }

    pub fn new(future: F) -> Self {
        Self::with_priority(future, 0)
    }

    fn future(self: Pin<&mut Self>) -> Pin<&mut F> {
        unsafe { self.map_unchecked_mut(|s| &mut s.future) }
    }

    fn started(self: Pin<&mut Self>) -> &mut bool {
        unsafe { &mut self.get_unchecked_mut().started }
    }

    fn context(self: Pin<&mut Self>) -> &mut Option<TxContext> {
        unsafe { &mut self.get_unchecked_mut().context }
    }
}

impl<F> Future for AsyncTx<F>
where
    F: Future,
{
    type Output = Result<F::Output, TxError>;

    fn poll(mut self: Pin<&mut Self>, future_ctx: &mut Context<'_>) -> Poll<Self::Output> {
        // insert new ctx
        let ctx = if !*self.as_mut().started() {
            *self.as_mut().started() = true;
            TxContext::new(self.priority)
        } else {
            self.as_mut()
                .context()
                .take()
                .expect("poll invoked for future that already finished")
        };
        let last_ctx = CUR_CTX.with(|cur_ctx| cur_ctx.borrow_mut().replace(ctx));

        // enter future body
        CUR_LVL.with(|cur_lvl| *cur_lvl.borrow_mut() += 1);
        let res = self.as_mut().future().poll(future_ctx);
        CUR_LVL.with(|cur_lvl| *cur_lvl.borrow_mut() -= 1);

        // restore old ctx
        let mut ctx = CUR_CTX
            .with(|cur_ctx| {
                if let Some(last_ctx) = last_ctx {
                    cur_ctx.borrow_mut().replace(last_ctx)
                } else {
                    cur_ctx.borrow_mut().take()
                }
            })
            .unwrap(); // unwrap should not fail since cur_ctx was inserted previously

        match res {
            Poll::Pending => {
                if let Some(err) = ctx.poll_error() {
                    Poll::Ready(Err(err))
                } else {
                    self.as_mut().context().replace(ctx);
                    Poll::Pending
                }
            }
            Poll::Ready(val) => {
                let res = if ctx.commit_and_forward() {
                    Ok(val)
                } else {
                    Err(TxError::Aborted)
                };
                Poll::Ready(res)
            }
        }
    }
}

#[macro_export]
macro_rules! async_tx {
    ($body: block) => {{
        use $crate::AsyncTx;
        AsyncTx::new(async move { $body })
    }};
    (repeat |$($p:ident),*| $body: block) => {async {
        use $crate::{AsyncTx, TxError};
        let mut output;
        loop {
            $(
                let mut $p = $p.handle();
            )*
            let res = $crate::async_tx!($body).await;
            match res {
                Err(TxError::Aborted) => {}
                Ok(o) => {output = o; break;}
            };

        }
        Ok::<_, TxError>(output)
    }};
}
