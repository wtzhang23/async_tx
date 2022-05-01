pub(crate) mod flags;

use crate::{
    data::containers::{Deed, TxBlockMap},
    transaction::error::TxError,
};
use flags::TxStaleFlag;
use std::{cell::RefCell, sync::Arc};

thread_local! {
    pub(crate) static CUR_CTX: RefCell<Option<TxContext>> = RefCell::new(None);
    pub(crate) static CUR_LVL: RefCell<usize> = RefCell::new(0);
}

pub(crate) fn cur_lvl() -> usize {
    CUR_LVL.with(|cur_lvl| *cur_lvl.borrow())
}

pub(crate) enum TxPendingType {
    Commit,
    Forward,
    Drop,
}

pub(crate) unsafe trait TxPending: Send {
    unsafe fn lock(&self);
    fn committable(&self) -> bool;
    unsafe fn abort_and_unlock(self: Box<Self>);
    unsafe fn commit_and_unlock(self: Box<Self>);
    fn lock_order(&self) -> usize;
    fn forwardable(&self) -> TxPendingType;
}

pub(crate) struct TxContext {
    pending: Vec<Box<dyn TxPending>>,
    pending_forwards: Vec<Box<dyn TxPending>>,
    stale_list: Vec<TxStaleFlag>,
    block_list: Vec<TxBlockMap>,
    error: Option<TxError>,
    deed: Arc<Deed>,
}

impl TxContext {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            pending_forwards: Vec::new(),
            stale_list: Vec::new(),
            block_list: Vec::new(),
            error: None,
            deed: Arc::new(Deed::new()),
        }
    }

    pub fn commit_and_forward(&mut self) -> bool {
        self.pending.sort_by_key(|pend| pend.lock_order());

        // phase 1: lock all locks
        self.pending.iter().for_each(|pend| unsafe { pend.lock() });

        // check if committable
        let committable = self.pending.iter().all(|pend| pend.committable());

        // phase 2: commit/abort then release locks
        if committable {
            self.pending
                .drain(..)
                .for_each(|pend| unsafe { pend.commit_and_unlock() });
        } else {
            self.pending
                .drain(..)
                .into_iter()
                .for_each(|pend| unsafe { pend.abort_and_unlock() });
        }

        if committable {
            CUR_CTX.with(|ctx| {
                // forward all pending operations to parent context
                self.pending_forwards.drain(..).for_each(|forward| {
                    let mut parent_ctx_opt = ctx.borrow_mut();
                    let parent_ctx = parent_ctx_opt.as_mut().unwrap(); // there should be no forwards to context not wrapped in a transaction
                    parent_ctx.add_pending(forward);
                });

                // forward all stale flags
                self.stale_list
                    .drain(..)
                    .filter(|flag| flag.forwardable())
                    .for_each(|flag| {
                        let mut parent_ctx_opt = ctx.borrow_mut();
                        let parent_ctx = parent_ctx_opt.as_mut().unwrap(); // there should be no forwards to context not wrapped in a transaction
                        parent_ctx.add_stale_flag(flag)
                    })
            });
        }
        committable
    }

    pub fn is_stale(&self) -> bool {
        self.stale_list.iter().any(|flag| flag.is_stale())
    }

    pub fn add_block_map(&mut self, block_map: TxBlockMap) {
        self.block_list.push(block_map);
    }

    pub fn signal_error(&mut self, err: TxError) {
        self.error.replace(err);
    }

    pub fn poll_error(&mut self) -> Option<TxError> {
        self.error.take()
    }

    pub fn deed(&self) -> Arc<Deed> {
        self.deed.clone()
    }

    pub fn unlock_all_locks(&mut self) {
        self.block_list.drain(..).for_each(|block_map| {
            if !block_map.stale() {
                block_map.unlock(0);
            }
        });
    }
}

impl TxContext {
    pub(crate) fn add_pending(&mut self, pending: Box<dyn TxPending>) {
        let pending_type = pending.forwardable();
        match pending_type {
            TxPendingType::Commit => self.pending.push(pending),
            TxPendingType::Forward => self.pending_forwards.push(pending),
            TxPendingType::Drop => {}
        };
    }

    pub(crate) fn add_stale_flag(&mut self, stale: TxStaleFlag) {
        self.stale_list.push(stale)
    }
}

impl Drop for TxContext {
    fn drop(&mut self) {
        self.unlock_all_locks();
    }
}

pub struct CommitGuard {
    level: usize,
}

impl CommitGuard {
    pub fn new() -> Self {
        Self { level: cur_lvl() }
    }
}

impl CommitGuard {
    pub(crate) fn lvl(&self) -> usize {
        self.level
    }
}
