use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::context::cur_lvl;

#[derive(Clone)]
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

#[derive(Clone)]
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

#[derive(Clone)]
pub struct TxLockFlag {
    flag: Arc<AtomicBool>,
}

impl TxLockFlag {
    pub(crate) fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn give_ownership(&self) {
        self.flag.store(true, Ordering::Release);
    }

    pub(crate) fn owned(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}
