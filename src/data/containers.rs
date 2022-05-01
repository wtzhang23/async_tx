use crate::{
    context::{
        flags::{TxLockFlag, TxStaleFlag},
        CUR_CTX,
    },
    transaction::condvar::TxWaitable,
};
use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, HashMap, VecDeque},
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
    type AccessFuture: Future<Output = ()>;
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
    type AccessFuture = Ready<()>;
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
        std::future::ready(())
    }

    fn get_data(&self) -> (Arc<Self::DataType>, TxStaleFlag) {
        (self.data.clone(), self.stale_flag.clone())
    }

    fn version(&self) -> usize {
        self.version
    }
}

type TxBlockingContainerLock<T> = parking_lot::Mutex<T>;

pub(crate) struct TxLockHandle(TxLockFlag, Waker, Arc<Deed>);

impl TxLockHandle {
    fn acquire_lock(self) -> Arc<Deed> {
        let Self(flag, waker, deed) = self;
        flag.give_ownership();
        waker.wake();
        deed
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

#[derive(Default)]
struct ConflictList {
    blocked: VecDeque<TxLockHandle>,
}

impl ConflictList {
    pub fn wake_all_after_commit(&mut self) {
        self.blocked.drain(..).for_each(|handle| {
            handle.acquire_lock();
        });
    }

    pub fn add_handle(&mut self, handle: TxLockHandle) {
        self.blocked.push_back(handle);
    }

    pub fn wake_one(&mut self) -> Arc<Deed> {
        debug_assert!(!self.blocked.is_empty());
        unsafe { self.blocked.pop_front().unwrap_unchecked().acquire_lock() }
    }

    pub fn is_empty(&self) -> bool {
        self.blocked.is_empty()
    }
}

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
        let successor_weak_ptr = Arc::downgrade(&successor).into_raw() as *mut Deed;
        self.blocked
            .get()
            .as_ref()
            .unwrap_unchecked() // should not fail since self.blocked guaranteed to not be null
            .swap(successor_weak_ptr, Ordering::AcqRel);

        if Arc::as_ptr(&successor) == self_raw {
            return true;
        }

        let mut cur = successor;
        loop {
            let next_ptr = cur
                .blocked
                .get()
                .as_ref()
                .unwrap_unchecked() // should not fail since cur.blocked guaranteed to not be null
                .load(Ordering::Acquire) as *const Deed;
            if next_ptr == std::ptr::null() {
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
        if weak_ptr != std::ptr::null_mut() {
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

struct PriorityData {
    deed: Arc<Deed>,
}

impl PriorityData {
    fn new(deed: Arc<Deed>) -> Self {
        Self { deed }
    }
}

enum TryLockStatus {
    Owned,
    Blocked,
    Ignored,
}

struct TxBlockMapInner {
    map: BTreeMap<usize, ConflictList>,
    priority_data: HashMap<usize, PriorityData>,
    stale: bool,
}

impl TxBlockMapInner {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            priority_data: HashMap::new(),
            stale: false,
        }
    }

    fn try_lock(&mut self, lock_handle: TxLockHandle, priority: usize) -> TryLockStatus {
        if !self.locked() || self.highest_priority() < priority || self.stale {
            self.priority_data
                .insert(priority, PriorityData::new(lock_handle.into_deed()));
            TryLockStatus::Owned
        } else {
            if let Some(data) = self.priority_data.get(&priority) {
                let has_cycle =
                    unsafe { lock_handle.deed().add_and_handle_cycle(data.deed.clone()) };
                if has_cycle {
                    return TryLockStatus::Ignored;
                }
            }

            self.map
                .entry(priority)
                .or_default()
                .add_handle(lock_handle);
            TryLockStatus::Blocked
        }
    }

    fn unlock(&mut self, priority: usize) {
        if self.stale {
            return;
        }

        // check if any other transaction with the same priority own the lock
        debug_assert!(self.priority_data.contains_key(&priority));
        let priority_data = unsafe { self.priority_data.remove(&priority).unwrap_unchecked() }; // key guaranteed to exist since transaction unlocking must have inserted itself into the map when invoking lock
        unsafe { priority_data.deed.remove_waiting_on() };
        if !self.locked() {
            self.wake_one();
        }
    }

    fn stale(&self) -> bool {
        self.stale
    }

    fn highest_priority(&self) -> usize {
        self.priority_data.keys().cloned().max().unwrap_or(0)
    }

    fn locked(&self) -> bool {
        !self.priority_data.is_empty()
    }

    fn wake_one(&mut self) {
        if let Some(max_prio) = self.map.keys().rev().next().cloned() {
            debug_assert!(!self.stale);
            let conflict_list = unsafe { self.map.get_mut(&max_prio).unwrap_unchecked() }; // key guaranteed to exist because was found through iterating through the map's keys
            let deed = conflict_list.wake_one();
            debug_assert!(!self.priority_data.contains_key(&max_prio));
            self.priority_data.insert(max_prio, PriorityData::new(deed));
            if conflict_list.is_empty() {
                self.map.remove(&max_prio);
            }
        }
    }

    fn wake_all_after_commit(&mut self) {
        self.stale = true;
        self.map.iter_mut().for_each(|(_prio, conflict_list)| {
            conflict_list.wake_all_after_commit();
        });
        self.priority_data.clear();
        atomic::fence(Ordering::Release);
    }
}

pub(crate) struct TxBlockMap {
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
        self.inner.lock().wake_all_after_commit();
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

    fn add_block_map(&self) {
        CUR_CTX.with(|cur_ctx| {
            cur_ctx
                .borrow_mut()
                .as_mut()
                .expect("TxBlocker must be used within an AsyncTx context")
                .add_block_map(self.block_map.clone())
        });
    }
}

impl Future for TxBlocker {
    type Output = ();

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
                    self.add_block_map();
                    return Poll::Ready(());
                }
                TryLockStatus::Blocked => {}
                TryLockStatus::Ignored => {
                    return Poll::Ready(());
                }
            }
        }

        if self.flag.owned() {
            self.add_block_map();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
