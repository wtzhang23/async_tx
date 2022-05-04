use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::context::cur_lvl;

// TODO: check if we can use nonatomic operations (i.e. RefCell) on these

#[derive(Debug, Clone)]
pub struct TxStaleFlag {
    flag: Arc<AtomicBool>,
    lvl: usize,
}

impl TxStaleFlag {
    pub(crate) fn is_stale(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    pub(crate) fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            lvl: cur_lvl(),
        }
    }

    pub(crate) fn mark_stale(&self) {
        self.flag.store(true, Ordering::Release);
    }

    pub(crate) fn forwardable(&self) -> bool {
        let lvl = cur_lvl();
        let barrier = self.lvl + 1;
        lvl > barrier
    }
}

#[derive(Clone, Debug)]
pub struct TxWaitFlag {
    flag: Arc<AtomicBool>,
}

impl TxWaitFlag {
    pub(crate) fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn wake(&self) {
        self.flag.store(true, Ordering::Release);
    }

    pub(crate) fn awake(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

pub(crate) enum LockStatus {
    Unowned = 0,
    Owned = 1,
    Stale = 2,
}

#[derive(Clone, Debug)]
pub struct TxLockFlag {
    flag: Arc<AtomicUsize>,
}

impl TxLockFlag {
    pub(crate) fn new() -> Self {
        Self {
            flag: Arc::new(AtomicUsize::new(LockStatus::Unowned as usize)),
        }
    }

    pub(crate) fn notify_ownership(&self) {
        self.flag
            .store(LockStatus::Owned as usize, Ordering::Relaxed);
    }

    pub(crate) fn notify_stale(&self) {
        self.flag
            .store(LockStatus::Stale as usize, Ordering::Relaxed);
    }

    pub(crate) fn status(&self) -> LockStatus {
        let status = self.flag.load(Ordering::Relaxed);
        if status == LockStatus::Owned as usize {
            LockStatus::Owned
        } else if status == LockStatus::Unowned as usize {
            LockStatus::Unowned
        } else if status == LockStatus::Stale as usize {
            LockStatus::Stale
        } else {
            unreachable!()
        }
    }
}
