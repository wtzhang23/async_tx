pub mod containers;

use crate::{
    context::flags::TxWaitFlag,
    context::{cur_lvl, CommitGuard, TxPending, TxPendingType, CUR_CTX},
    transaction::condvar::TxWaitable,
    transaction::error::{TxError, TxErrorPropagator},
};
use containers::TxDataContainer;
use lock_api::RwLock;
use lock_api::{RawRwLock, RwLockUpgradableReadGuard};
use std::{sync::atomic, sync::atomic::Ordering, sync::Arc, task::Waker};

use self::containers::{TxBlockingContainer, TxNonblockingContainer};

type TxRwLock<T> = RwLock<parking_lot::RawRwLock, T>;
pub type TxNonblockingData<T> = TxData<TxNonblockingContainer<T>>;
pub type TxBlockingData<T> = TxData<TxBlockingContainer<T>>;

pub struct TxData<C> {
    most_recent: Arc<TxRwLock<C>>,
}

impl<C> TxData<C>
where
    C: TxDataContainer + Send + Sync,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    pub fn new(data: C::DataType) -> Self {
        Self {
            most_recent: Arc::new(TxRwLock::new(C::new(data))),
        }
    }

    pub fn handle(&self) -> TxDataHandle<C> {
        self.handle_for_guard(&CommitGuard::new())
    }

    pub fn handle_for_guard(&self, commit_guard: &CommitGuard) -> TxDataHandle<C> {
        TxDataHandle::new(self.most_recent.clone(), commit_guard)
    }
}

enum ArcCow<T> {
    Shared(Arc<T>),
    Owned(T),
    Unresolved,
}

impl<T> ArcCow<T> {
    async fn upgrade_readable<C>(&mut self, tx_data: &TxRwLock<C>) -> (&T, Option<usize>)
    where
        C: TxDataContainer<DataType = T>,
    {
        let mut last_version = None;
        if let Self::Unresolved = self {
            let flag = {
                let guard = tx_data.read();
                last_version = Some(guard.version());
                let (data, flag) = guard.get_data();
                *self = Self::Shared(data);

                if C::needs_access() {
                    // gaining access must be done after acquiring arc as lock must be dropped after awaiting
                    {
                        let guard = guard;
                        guard.gain_access()
                    }
                    .await;
                }
                flag
            };
            if CUR_CTX.with(|ctx| {
                let mut ctx_ref = ctx.borrow_mut();
                let ctx = ctx_ref.as_mut().unwrap();
                ctx.add_stale_flag(flag);
                ctx.is_stale()
            }) {
                TxErrorPropagator::new(TxError::Aborted).await;
            }
        }
        (
            match self {
                Self::Shared(arc) => &*arc,
                Self::Owned(val) => val,
                Self::Unresolved => unreachable!(),
            },
            last_version,
        )
    }

    fn override_to_writable(&mut self, data: T) -> &mut T {
        *self = Self::Owned(data);
        if let Self::Owned(data) = self {
            data
        } else {
            unreachable!();
        }
    }

    fn write_pending(&self) -> bool {
        if let ArcCow::Owned(_) = self {
            true
        } else {
            false
        }
    }

    fn pending_val(self) -> Option<T> {
        if let ArcCow::Owned(val) = self {
            Some(val)
        } else {
            None
        }
    }
}

impl<T> ArcCow<T>
where
    T: Clone,
{
    async fn upgrade_writable<C>(&mut self, tx_data: &TxRwLock<C>) -> (&mut T, Option<usize>)
    where
        C: TxDataContainer<DataType = T>,
    {
        match self {
            Self::Unresolved => {
                let (val, version) = self.upgrade_readable(tx_data).await;
                let val = val.clone();
                (self.override_to_writable(val), version)
            }
            Self::Shared(shared) => {
                let val = shared.as_ref().clone();
                (self.override_to_writable(val), None)
            }
            Self::Owned(val) => (val, None),
        }
    }
}

pub struct TxReadData<C>
where
    C: TxDataContainer + 'static,
{
    data: Arc<TxRwLock<C>>,
    read_version: Option<usize>,
}

impl<C> TxReadData<C>
where
    C: TxDataContainer + 'static,
{
    fn new(data: Arc<TxRwLock<C>>) -> Self {
        Self {
            data,
            read_version: None,
        }
    }

    fn set_version(&mut self, version: usize) {
        self.read_version = Some(version);
    }

    fn version(&self) -> Option<usize> {
        self.read_version.clone()
    }

    fn data(&self) -> &Arc<TxRwLock<C>> {
        &self.data
    }
}

impl<C> Clone for TxReadData<C>
where
    C: TxDataContainer + 'static,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            read_version: self.read_version.clone(),
        }
    }
}

pub(crate) struct TxDataHandleInner<C>
where
    C: TxDataContainer + 'static,
{
    read_data: TxReadData<C>,
    local_data: ArcCow<C::DataType>,
    commit_level: usize,
}

impl<C> TxDataHandleInner<C>
where
    C: TxDataContainer + 'static,
{
    fn new(data: Arc<TxRwLock<C>>, commit_guard: &CommitGuard) -> Self {
        let commit_level = commit_guard.lvl();
        Self {
            read_data: TxReadData::new(data),
            commit_level,
            local_data: ArcCow::Unresolved,
        }
    }

    async fn read(&mut self) -> &C::DataType {
        if CUR_CTX.with(|cur_ctx| cur_ctx.borrow().is_none()) {
            unimplemented!("Unsupported read outside of a Transaction context")
        }

        let (reference, version) = self
            .local_data
            .upgrade_readable(&*self.read_data.data())
            .await;
        if let Some(version) = version {
            self.read_data.set_version(version);
        }
        reference
    }

    fn set(&mut self, val: C::DataType) -> &mut C::DataType {
        if CUR_CTX.with(|cur_ctx| cur_ctx.borrow().is_none()) {
            unimplemented!("Unsupported set outside of a Transaction context")
        }

        self.local_data.override_to_writable(val)
    }
}

impl<C> TxDataHandleInner<C>
where
    C: TxDataContainer + 'static,
    <C as TxDataContainer>::DataType: Clone,
{
    async fn write(&mut self) -> &mut C::DataType {
        if CUR_CTX.with(|cur_ctx| cur_ctx.borrow().is_none()) {
            unimplemented!("Unsupported write outside of a Transaction context")
        }

        let (reference, version) = self
            .local_data
            .upgrade_writable(&*self.read_data.data())
            .await;
        if let Some(version) = version {
            self.read_data.set_version(version);
        }
        reference
    }
}

unsafe impl<C> TxPending for TxDataHandleInner<C>
where
    C: TxDataContainer + Sync + Send + 'static,
    <C as TxDataContainer>::DataType: Sync + Send,
{
    unsafe fn lock(&self) {
        if self.local_data.write_pending() {
            self.read_data.data().raw().lock_exclusive()
        }
    }

    fn committable(&self) -> bool {
        if let Some(version) = self.read_data.version() {
            unsafe {
                atomic::fence(Ordering::Acquire);
                let inner = self.read_data.data().data_ptr().as_ref().unwrap_unchecked(); // internal lock's value must not be null
                inner.version() == version
            }
        } else {
            true
        }
    }

    unsafe fn abort_and_unlock(self: Box<Self>) {
        if self.local_data.write_pending() {
            debug_assert!(self.read_data.data().raw().is_locked());
            self.read_data.data().raw().unlock_exclusive()
        }
    }

    unsafe fn commit_and_unlock(self: Box<Self>) {
        if let Some(new_data) = self.local_data.pending_val() {
            debug_assert!(self.read_data.data().raw().is_locked());
            let mut_inner = self.read_data.data().data_ptr().as_mut().unwrap_unchecked(); // internal lock's value must not be null
            mut_inner.commit(new_data);
            self.read_data.data().raw().unlock_exclusive();
        }
    }

    fn lock_order(&self) -> usize {
        Arc::as_ptr(&self.read_data.data()) as usize
    }

    fn forwardable(&self) -> TxPendingType {
        let lvl = cur_lvl();
        let barrier = self.commit_level + 1;
        if lvl == barrier {
            TxPendingType::Commit
        } else if lvl < barrier {
            TxPendingType::Drop
        } else {
            TxPendingType::Forward
        }
    }
}

pub struct TxDataHandle<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    inner: Option<TxDataHandleInner<C>>,
}

impl<C> TxDataHandle<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    fn new(data: Arc<TxRwLock<C>>, commit_guard: &CommitGuard) -> Self {
        Self {
            inner: Some(TxDataHandleInner::new(data, commit_guard)),
        }
    }
}

impl<C> TxDataHandle<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    pub(crate) fn wait_handle(&self) -> TxDataWaiter<C> {
        TxDataWaiter::new(self.inner.as_ref().unwrap().read_data.clone())
    }
}

impl<C> TxDataHandle<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    pub async fn read(&mut self) -> &C::DataType {
        self.inner.as_mut().unwrap().read().await
    }

    pub async fn set(&mut self, val: C::DataType) -> &mut C::DataType {
        self.inner.as_mut().unwrap().set(val)
    }

    pub fn write_pending(&self) -> bool {
        self.inner.as_ref().unwrap().local_data.write_pending()
    }
}

impl<C> TxDataHandle<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync + Clone,
{
    pub async fn write(&mut self) -> &mut C::DataType {
        self.inner.as_mut().unwrap().write().await
    }
}

impl<C> Drop for TxDataHandle<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    fn drop(&mut self) {
        CUR_CTX.with(|ctx| {
            if let Some(cur_ctx) = ctx.borrow_mut().as_mut() {
                cur_ctx.add_pending(Box::new(self.inner.take().unwrap()))
            }
        })
    }
}

pub(crate) struct TxDataWaiter<C>
where
    C: TxDataContainer + 'static,
{
    read_data: TxReadData<C>,
    waker_and_flag: Option<(Waker, TxWaitFlag)>,
}

impl<C> TxDataWaiter<C>
where
    C: TxDataContainer + 'static,
{
    pub fn new(read_data: TxReadData<C>) -> Self {
        Self {
            read_data,
            waker_and_flag: None,
        }
    }
}

impl<C> TxWaitable for TxDataWaiter<C>
where
    C: TxDataContainer + Send + Sync + 'static,
    <C as TxDataContainer>::DataType: Send + Sync,
{
    fn enqueue_wait(mut self: Box<Self>, waker: Waker, flag: TxWaitFlag) {
        if let Some(version) = self.read_data.version() {
            unsafe {
                let guard = Arc::as_ptr(self.read_data.data())
                    .as_ref()
                    .unwrap()
                    .upgradable_read(); // arc owned by self guarantees that data won't be freed until end of function call

                if guard.version() > version {
                    flag.wake();
                    waker.wake();
                } else {
                    debug_assert_eq!(guard.version(), version);
                    let mut guard = RwLockUpgradableReadGuard::upgrade(guard);
                    self.waker_and_flag.replace((waker, flag));
                    guard.add_waiter(*self);
                }
            }
        } else {
            unreachable!();
        }
    }

    fn wake(self: Box<Self>) {
        if let Some((waker, flag)) = self.waker_and_flag {
            flag.wake();
            waker.wake();
        }
    }
}