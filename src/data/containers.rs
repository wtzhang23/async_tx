use crate::context::flags::LockStatus;
use crate::context::signal_err;
use crate::TxError;
use crate::{
    context::{
        flags::{TxLockFlag, TxStaleFlag},
        CUR_CTX,
    },
    transaction::condvar::TxWaitable,
};
use std::{
    cell::UnsafeCell,
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::{Future, Ready},
    pin::Pin,
    sync::{
        atomic::{self, AtomicPtr, Ordering},
        Arc, Weak,
    },
    task::{Context, Poll, Waker},
};

pub trait TxDataContainer {
    type DataType;
    type AccessFuture: Future<Output = Option<TxBlockMap>>;
    fn new(data: Self::DataType) -> Self;
    fn commit(&mut self, new_data: Self::DataType);
    fn add_waiter<W: TxWaitable + Send + Sync + 'static>(&mut self, waiter: W);
    fn needs_access() -> bool;
    fn gain_access(&self) -> Self::AccessFuture;
    fn get_data(&self) -> (Arc<Self::DataType>, TxStaleFlag);
    fn version(&self) -> usize;
}

pub struct TxNonblockingContainer<T> {
    data: Arc<T>,
    version: usize,
    stale_flag: TxStaleFlag,
    waiters: Vec<Box<dyn TxWaitable + Send + Sync>>,
}

impl<T> TxDataContainer for TxNonblockingContainer<T> {
    type DataType = T;
    type AccessFuture = Ready<Option<TxBlockMap>>;
    fn new(data: Self::DataType) -> Self {
        Self {
            data: Arc::new(data),
            version: 0,
            stale_flag: TxStaleFlag::new(),
            waiters: Vec::new(),
        }
    }

    fn commit(&mut self, new_data: Self::DataType) {
        self.data = Arc::new(new_data);
        self.version += 1;

        // signal that value is stale
        self.stale_flag.mark_stale();
        self.stale_flag = TxStaleFlag::new();

        // wake up waiters
        for waiter in self.waiters.drain(..) {
            waiter.wake()
        }
        self.waiters.clear();
    }

    fn add_waiter<W: TxWaitable + Send + Sync + 'static>(&mut self, waiter: W) {
        self.waiters.push(Box::new(waiter))
    }

    fn needs_access() -> bool {
        false
    }

    fn gain_access(&self) -> Self::AccessFuture {
        std::future::ready(None)
    }

    fn get_data(&self) -> (Arc<Self::DataType>, TxStaleFlag) {
        (self.data.clone(), self.stale_flag.clone())
    }

    fn version(&self) -> usize {
        self.version
    }
}

impl<T> Debug for TxNonblockingContainer<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxNonblockingContainer")
            .field("data", &self.data)
            .field("version", &self.version)
            .field("stale_flag", &self.stale_flag)
            .field("waiters", &"<opaque>")
            .finish()
    }
}

type TxBlockingContainerLock<T> = parking_lot::Mutex<T>;

#[derive(Debug)]
pub(crate) struct TxLockHandle(TxLockFlag, Waker, Arc<Deed>);

impl TxLockHandle {
    fn acquire_lock(self) -> Arc<Deed> {
        let Self(flag, waker, deed) = self;
        flag.notify_ownership();
        waker.wake();
        deed
    }

    fn notify_stale(self) {
        let Self(flag, waker, _deed) = self;
        flag.notify_stale();
        waker.wake();
    }

    fn into_deed(self) -> Arc<Deed> {
        let Self(_flag, _waker, deed) = self;
        deed
    }

    fn deed(&self) -> &Arc<Deed> {
        let Self(_flag, _waker, deed) = self;
        deed
    }
}

#[derive(Default, Debug)]
struct ConflictList {
    blocked: VecDeque<TxLockHandle>,
    owning_deed: Option<Arc<Deed>>,
}

impl ConflictList {
    pub fn new(owning_deed: Arc<Deed>) -> Self {
        Self {
            blocked: VecDeque::new(),
            owning_deed: Some(owning_deed),
        }
    }

    pub fn unlock_all_after_commit(&mut self) {
        self.blocked
            .drain(..)
            .for_each(|handle| handle.notify_stale());
    }

    pub fn add_handle(&mut self, handle: TxLockHandle) {
        self.blocked.push_back(handle);
    }

    unsafe fn wake_one(&mut self) {
        debug_assert!(!self.blocked.is_empty());
        debug_assert!(self.owning_deed.is_none());
        let to_wake = self.blocked.pop_front().unwrap_unchecked();
        self.owning_deed = Some(to_wake.acquire_lock());
    }

    pub fn owning_deed(&mut self) -> Option<&Arc<Deed>> {
        self.owning_deed.as_ref()
    }

    pub unsafe fn clear_deed(&mut self) {
        debug_assert!(self.owning_deed.is_some());
        self.owning_deed
            .take()
            .unwrap_unchecked()
            .remove_waiting_on();
    }

    pub fn no_waiters(&self) -> bool {
        self.blocked.is_empty()
    }
}

#[derive(Debug)]
pub(crate) struct Deed {
    blocked: UnsafeCell<AtomicPtr<Deed>>, // TODO: check if atomic ptr can be removed; need memory fence to flush write buffers
}

impl Deed {
    pub fn new() -> Self {
        Self {
            blocked: UnsafeCell::new(AtomicPtr::new(std::ptr::null_mut())),
        }
    }
}

impl Deed {
    unsafe fn add_and_handle_cycle(self: &Arc<Self>, successor: Arc<Self>) -> bool {
        let self_raw = Arc::as_ptr(self);
        if Arc::as_ptr(&successor) == self_raw {
            return true;
        }

        let successor_weak_ptr = Arc::downgrade(&successor).into_raw() as *mut Deed;
        self.blocked
            .get()
            .as_ref()
            .unwrap_unchecked() // should not fail since self.blocked guaranteed to not be null
            .swap(successor_weak_ptr, Ordering::AcqRel);

        let mut cur = successor;
        loop {
            let next_ptr = cur
                .blocked
                .get()
                .as_ref()
                .unwrap_unchecked() // should not fail since cur.blocked guaranteed to not be null
                .load(Ordering::Acquire) as *const Deed;
            if next_ptr.is_null() {
                return false;
            } else if next_ptr == self_raw {
                self.remove_waiting_on();
                return true;
            }

            let weak = Weak::from_raw(next_ptr);
            let upgraded = weak.upgrade();
            #[allow(unused_must_use)]
            {
                weak.into_raw();
            }

            if let Some(next) = upgraded {
                cur = next;
            } else {
                return false;
            }
        }
    }

    unsafe fn remove_waiting_on(&self) {
        let weak_ptr = self
            .blocked
            .get()
            .as_ref()
            .unwrap_unchecked() // should not be null since self.blocked guaranteed to not be null
            .swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !weak_ptr.is_null() {
            Weak::from_raw(weak_ptr); // acquire weak to drop reference count
        }
    }
}

impl Drop for Deed {
    fn drop(&mut self) {
        unsafe { self.remove_waiting_on() };
    }
}

unsafe impl Sync for Deed {}

enum TryLockStatus {
    Owned,
    Blocked,
    Ignored,
    Stale,
}

#[derive(Debug)]
struct TxBlockMapInner {
    priority_map: HashMap<usize, ConflictList>,
    stale: bool,
}

impl TxBlockMapInner {
    fn new() -> Self {
        Self {
            priority_map: HashMap::new(),
            stale: false,
        }
    }

    fn try_lock(&mut self, lock_handle: TxLockHandle, priority: usize) -> TryLockStatus {
        if self.stale {
            TryLockStatus::Stale
        } else if !self.locked() || self.highest_priority() < priority {
            let prev = self
                .priority_map
                .insert(priority, ConflictList::new(lock_handle.into_deed()));
            debug_assert!(prev.is_none());
            TryLockStatus::Owned
        } else {
            let conflict_list = self.priority_map.entry(priority).or_default();
            if let Some(owning_deed) = conflict_list.owning_deed() {
                let has_cycle =
                    unsafe { lock_handle.deed().add_and_handle_cycle(owning_deed.clone()) };
                if has_cycle {
                    return TryLockStatus::Ignored;
                }
            }
            conflict_list.add_handle(lock_handle);
            TryLockStatus::Blocked
        }
    }

    fn unlock(&mut self, priority: usize) {
        if self.stale {
            return;
        }

        // check if any other transaction with the same priority own the lock
        debug_assert!(self.priority_map.contains_key(&priority)); // key guaranteed to exist since transaction unlocking must have inserted itself into the map when invoking lock
        let conflict_list = unsafe { self.priority_map.get_mut(&priority).unwrap_unchecked() };
        unsafe { conflict_list.clear_deed() };

        // remove to preserve invariant that the conflict with the maximum priority has waiters
        if conflict_list.no_waiters() {
            self.priority_map.remove(&priority);
        }

        if !self.locked() {
            let max_prio = self.highest_priority();
            if let Some(conflict_list) = self.priority_map.get_mut(&max_prio) {
                debug_assert!(!self.stale);
                unsafe { conflict_list.wake_one() };
            }
        }
    }

    fn unlock_all_after_commit(&mut self) {
        debug_assert!(!self.stale);
        self.stale = true;
        self.priority_map
            .iter_mut()
            .for_each(|(_prio, conflict_list)| {
                conflict_list.unlock_all_after_commit();
            });
        atomic::fence(Ordering::Release);
    }

    fn stale(&self) -> bool {
        self.stale
    }

    fn highest_priority(&self) -> usize {
        self.priority_map.keys().cloned().max().unwrap_or(0)
    }

    fn locked(&mut self) -> bool {
        let highest_priority = self.highest_priority();
        if let Some(conflict_list) = self.priority_map.get_mut(&highest_priority) {
            let locked = conflict_list.owning_deed().is_some();
            locked
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct TxBlockMap {
    inner: Arc<TxBlockingContainerLock<TxBlockMapInner>>,
}

impl TxBlockMap {
    fn new() -> Self {
        Self {
            inner: Arc::new(TxBlockingContainerLock::new(TxBlockMapInner::new())),
        }
    }

    fn try_lock(&self, lock_handle: TxLockHandle, priority: usize) -> TryLockStatus {
        self.inner.lock().try_lock(lock_handle, priority)
    }
}

impl TxBlockMap {
    pub(crate) fn wake_all_after_commit(&self) {
        self.inner.lock().unlock_all_after_commit();
    }

    pub(crate) fn unlock(&self, priority: usize) {
        self.inner.lock().unlock(priority);
    }

    pub(crate) fn stale(&self) -> bool {
        atomic::fence(Ordering::Acquire);
        unsafe {
            self.inner
                .data_ptr()
                .as_ref()
                .unwrap_unchecked() // value guaranteed to not be null since it was present in the mutex
                .stale()
        }
    }
}

impl Clone for TxBlockMap {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct TxBlockingContainer<T> {
    nonblocking: TxNonblockingContainer<T>,
    block_map: TxBlockMap,
}

impl<T> TxDataContainer for TxBlockingContainer<T> {
    type DataType = T;
    type AccessFuture = TxBlocker;

    fn new(data: Self::DataType) -> Self {
        Self {
            nonblocking: TxNonblockingContainer::new(data),
            block_map: TxBlockMap::new(),
        }
    }

    fn commit(&mut self, new_data: Self::DataType) {
        self.nonblocking.commit(new_data);
        self.block_map.wake_all_after_commit();
        self.block_map = TxBlockMap::new();
    }

    fn add_waiter<W: TxWaitable + Send + Sync + 'static>(&mut self, waiter: W) {
        self.nonblocking.add_waiter(waiter)
    }

    fn needs_access() -> bool {
        true
    }

    fn gain_access(&self) -> Self::AccessFuture {
        TxBlocker::new(self.block_map.clone())
    }

    fn get_data(&self) -> (Arc<Self::DataType>, TxStaleFlag) {
        self.nonblocking.get_data()
    }

    fn version(&self) -> usize {
        self.nonblocking.version()
    }
}

impl<T> Debug for TxBlockingContainer<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxBlockingContainer")
            .field("nonblocking", &self.nonblocking)
            .field("block_map", &self.block_map)
            .finish()
    }
}

pub struct TxBlocker {
    block_map: TxBlockMap,
    tried: bool,
    flag: TxLockFlag,
}

impl TxBlocker {
    pub(crate) fn new(inner: TxBlockMap) -> Self {
        let flag = TxLockFlag::new();
        Self {
            block_map: inner,
            tried: false,
            flag,
        }
    }
}

impl Future for TxBlocker {
    type Output = Option<TxBlockMap>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.tried {
            let flag = self.flag.clone();
            let (deed, priority) = CUR_CTX.with(|ctx| {
                let ctx_ref = ctx.borrow();
                let cur_ctx = ctx_ref
                    .as_ref()
                    .expect("TxBlocker must be used within an AsyncTx context");
                (cur_ctx.deed(), cur_ctx.priority())
            });
            let success = self
                .block_map
                .try_lock(TxLockHandle(flag, cx.waker().clone(), deed), priority);
            self.tried = true;
            match success {
                TryLockStatus::Owned => {
                    return Poll::Ready(Some(self.block_map.clone()));
                }
                TryLockStatus::Blocked => {}
                TryLockStatus::Ignored => {
                    return Poll::Ready(None);
                }
                TryLockStatus::Stale => {
                    unsafe { signal_err(TxError::Aborted) };
                    return Poll::Pending;
                }
            }
        }

        match self.flag.status() {
            LockStatus::Unowned => Poll::Pending,
            LockStatus::Owned => Poll::Ready(Some(self.block_map.clone())),
            LockStatus::Stale => {
                unsafe { signal_err(TxError::Aborted) };
                Poll::Pending
            }
        }
    }
}
