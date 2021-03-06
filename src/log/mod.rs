pub mod collections;

use std::sync::Arc;

use crate::context::{TxPending, CUR_CTX};
use crate::data::containers::TxDataContainer;
use crate::data::{TxData, TxDataHandle};
use crate::CommitGuard;

#[derive(Debug)]
pub struct TxLogHead<Record> {
    top: Option<Arc<TxLogEntry<Record>>>,
}

impl<Record> TxLogHead<Record> {
    fn new(entry: TxLogEntry<Record>) -> Self {
        Self {
            top: Some(Arc::new(entry)),
        }
    }

    fn apply_on_log<F>(&self, apply: F)
    where
        F: FnMut(&Arc<TxLogEntry<Record>>) -> bool,
    {
        if let Some(top) = &self.top {
            top.apply_on_list(apply);
        }
    }
}

impl<Record> Default for TxLogHead<Record> {
    fn default() -> Self {
        Self { top: None }
    }
}

impl<Record> Clone for TxLogHead<Record> {
    fn clone(&self) -> Self {
        Self {
            top: self.top.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TxLogEntry<Record> {
    record: Record,
    prev: Option<Arc<TxLogEntry<Record>>>,
}

impl<Record> TxLogEntry<Record> {
    pub fn new<I>(record: Record, prev: I) -> Self
    where
        I: Into<Option<Arc<TxLogEntry<Record>>>>,
    {
        Self {
            record,
            prev: prev.into(),
        }
    }

    pub fn record(&self) -> &Record {
        &self.record
    }

    pub fn apply_on_prevs<'a, F>(&'a self, mut apply: F)
    where
        F: FnMut(&'a Arc<Self>) -> bool,
    {
        if let Some(prev) = &self.prev {
            if apply(prev) {
                let mut cur = prev;
                while let Some(prev) = &cur.prev {
                    if !apply(prev) {
                        break;
                    }
                    cur = prev;
                }
            }
        }
    }

    pub fn apply_on_list<'a, F>(self: &'a Arc<Self>, mut apply: F)
    where
        F: FnMut(&'a Arc<Self>) -> bool,
    {
        if apply(self) {
            self.apply_on_prevs(apply);
        }
    }
}

pub trait TxLogView: Default + Into<TxLogEntry<Self::Record>> {
    type Record;
    fn consume_prev(&mut self, entry: &Arc<TxLogEntry<Self::Record>>) -> bool;
}

#[derive(Debug)]
pub struct TxLogStructure<Record, C>
where
    C: TxDataContainer<DataType = TxLogHead<Record>>,
{
    log_head: TxData<C>,
}

impl<Record, C> TxLogStructure<Record, C>
where
    C: TxDataContainer<DataType = TxLogHead<Record>>,
{
    pub fn new() -> Self {
        Self {
            log_head: TxData::new(TxLogHead::default()),
        }
    }

    pub fn handle<View: TxLogView<Record = Record>>(
        &self,
    ) -> TxLogStructureHandle<Record, C, View> {
        self.log_head.handle().into()
    }

    pub fn handle_for_guard<View: TxLogView<Record = Record>>(
        &self,
        commit_guard: CommitGuard,
    ) -> TxLogStructureHandle<Record, C, View> {
        self.log_head.handle_for_guard(commit_guard).into()
    }
}

impl<Record, C> Default for TxLogStructure<Record, C>
where
    C: TxDataContainer<DataType = TxLogHead<Record>>,
{
    fn default() -> Self {
        Self::new()
    }
}

enum ViewStatus<View>
where
    View: TxLogView,
{
    Unresolved,
    Read(View),
    Write(View),
}

struct TxLogStructureHandleInner<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    log_handle: TxDataHandle<C>,
    view_status: ViewStatus<View>,
}

impl<Record, C, View> TxLogStructureHandleInner<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    pub async fn read(&mut self) -> &View {
        if matches!(&self.view_status, ViewStatus::Unresolved) {
            let head = self.log_handle.read().await;
            let mut view = View::default();

            head.apply_on_log(|entry| view.consume_prev(entry));
            self.view_status = ViewStatus::Read(view);
        }
        match &self.view_status {
            ViewStatus::Unresolved => unreachable!(),
            ViewStatus::Read(view) => view,
            ViewStatus::Write(view) => view,
        }
    }
}

impl<Record, C, View> TxLogStructureHandleInner<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    pub async fn write(&mut self) -> &mut View {
        self.read().await;
        self.log_handle.write().await; // signal that log_handle has writes pending
        if matches!(&self.view_status, ViewStatus::Read(_)) {
            let prev = std::mem::replace(&mut self.view_status, ViewStatus::Unresolved);
            if let ViewStatus::Read(view) = prev {
                self.view_status = ViewStatus::Write(view);
            } else {
                unreachable!()
            }
        }
        if let ViewStatus::Write(view) = &mut self.view_status {
            view
        } else {
            unreachable!()
        }
    }
}

impl<Record, C, View> From<TxDataHandle<C>> for TxLogStructureHandleInner<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    fn from(val: TxDataHandle<C>) -> Self {
        Self {
            log_handle: val,
            view_status: ViewStatus::Unresolved,
        }
    }
}

unsafe impl<Record, C, View> TxPending for TxLogStructureHandleInner<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    unsafe fn lock(&self) {
        self.log_handle.inner().lock();
    }

    fn committable(&self) -> bool {
        self.log_handle.inner().committable()
    }

    unsafe fn abort_and_unlock(&mut self) {
        let mut inner = self.log_handle.take_inner();
        inner.abort_and_unlock();
    }

    unsafe fn commit_and_unlock(&mut self) {
        // propagate writes
        let status = std::mem::replace(&mut self.view_status, ViewStatus::Unresolved);
        if let ViewStatus::Write(view) = status {
            self.log_handle.inner_mut().set(TxLogHead::new(view.into()));
        }
        let mut inner = self.log_handle.take_inner();
        inner.commit_and_unlock();
    }

    fn lock_order(&self) -> usize {
        self.log_handle.inner().lock_order()
    }

    fn forwardable(&self) -> crate::context::TxPendingType {
        self.log_handle.inner().forwardable()
    }
}

pub struct TxLogStructureHandle<Record, C, View>
where
    Record: 'static,
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record> + 'static,
{
    inner: Option<TxLogStructureHandleInner<Record, C, View>>,
}

impl<Record, C, View> TxLogStructureHandle<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    fn inner_mut(&mut self) -> &mut TxLogStructureHandleInner<Record, C, View> {
        debug_assert!(self.inner.is_some());
        unsafe { self.inner.as_mut().unwrap_unchecked() }
    }

    pub async fn read(&mut self) -> &View {
        self.inner_mut().read().await
    }
}

impl<Record, C, View> TxLogStructureHandle<Record, C, View>
where
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record>,
{
    pub async fn write(&mut self) -> &mut View {
        self.inner_mut().write().await
    }
}

impl<Record, C, View> Drop for TxLogStructureHandle<Record, C, View>
where
    Record: 'static,
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record> + 'static,
{
    fn drop(&mut self) {
        CUR_CTX
            .try_with(|ctx| {
                // drop can occur outside of AsyncTx context; i.e. the transaction be dropped before completion
                if let Some(cur_ctx) = ctx.borrow_mut().as_mut() {
                    if let Some(inner) = self.inner.take() {
                        cur_ctx.add_pending(Box::new(inner))
                    }
                }
            })
            .ok();
    }
}

impl<Record, C, View> From<TxDataHandle<C>> for TxLogStructureHandle<Record, C, View>
where
    Record: 'static,
    C: TxDataContainer<DataType = TxLogHead<Record>> + 'static,
    View: TxLogView<Record = Record> + 'static,
{
    fn from(val: TxDataHandle<C>) -> Self {
        Self {
            inner: Some(TxLogStructureHandleInner::from(val)),
        }
    }
}
