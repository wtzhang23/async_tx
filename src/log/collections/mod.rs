use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::data::containers::{TxBlockingContainer, TxNonblockingContainer};
use crate::CommitGuard;

use super::{TxLogEntry, TxLogHead, TxLogStructure, TxLogStructureHandle, TxLogView};

pub trait KeyValueCollection<K, V>: Default {
    fn with_capacity(size: usize) -> Self;
    fn reserve(&mut self, additional: usize);
    fn shrink_to_fit(&mut self);

    fn insert(&mut self, key: K, val: V) -> Option<V>;
    fn remove(&mut self, key: &K) -> Option<V>;
    fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>;
    fn get_mut(&mut self, key: &K) -> Option<&mut V>;
    fn len(&self) -> usize;
    fn iter_key_values<F: FnMut(&K, &V)>(&self, iter_fn: F);

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.get(key).is_some()
    }
}

pub enum KeyValueRecord<K, V, Collection>
where
    Collection: KeyValueCollection<K, Option<V>>,
{
    Segment {
        collection: Collection,
        num_added: usize,
        num_removed: usize,
        _phantom_key: PhantomData<K>,
        _phantom_val: PhantomData<V>,
    },
}

impl<K, V, Collection> Debug for KeyValueRecord<K, V, Collection>
where
    Collection: KeyValueCollection<K, Option<V>> + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Segment {
                collection,
                num_added,
                num_removed,
                _phantom_key,
                _phantom_val,
            } => f
                .debug_struct("Segment")
                .field("collection", collection)
                .field("num_added", num_added)
                .field("num_removed", num_removed)
                .finish(),
        }
    }
}

pub struct KeyValueView<K, V, Collection>
where
    Collection: KeyValueCollection<K, Option<V>>,
{
    local_collection: Collection,
    prev: Option<Arc<TxLogEntry<KeyValueRecord<K, V, Collection>>>>,
    num_added: usize,
    num_removed: usize,
}

impl<K, V, Collection> KeyValueView<K, V, Collection>
where
    K: Clone + Eq + Hash,
    Collection: KeyValueCollection<K, Option<V>>,
{
    fn apply_on_prevs<'a, F>(&'a self, apply: F)
    where
        F: FnMut(&'a Arc<TxLogEntry<KeyValueRecord<K, V, Collection>>>) -> bool,
    {
        if let Some(prev) = &self.prev {
            prev.apply_on_list(apply);
        }
    }

    fn visit<F>(key: &K, val: &Option<V>, visited: &mut HashSet<K>, visit_fn: &mut F)
    where
        F: FnMut(&K, &V),
    {
        if !visited.contains(key) {
            visited.insert(key.clone());
            if let Some(val) = val.as_ref() {
                visit_fn(key, val);
            }
        }
    }

    pub fn log_size(&self) -> usize {
        let mut size = 0;
        self.apply_on_prevs(|_entry| {
            size += 1;
            true
        });
        size
    }
}

impl<K, V, Collection> TxLogView for KeyValueView<K, V, Collection>
where
    K: Clone + Hash + Eq,
    V: Clone,
    Collection: KeyValueCollection<K, Option<V>>,
{
    type Record = KeyValueRecord<K, V, Collection>;

    fn consume_prev(&mut self, entry: &Arc<TxLogEntry<Self::Record>>) -> bool {
        self.prev = Some(entry.clone());
        false
    }
}

fn normalize_add_remove(added: usize, removed: usize) -> (usize, usize) {
    let new_added;
    let new_removed;
    if added > removed {
        new_added = added - removed;
        new_removed = 0;
    } else {
        new_added = 0;
        new_removed = removed - added;
    }
    (new_added, new_removed)
}

#[allow(clippy::from_over_into)]
impl<K, V, Collection> Into<TxLogEntry<KeyValueRecord<K, V, Collection>>>
    for KeyValueView<K, V, Collection>
where
    K: Hash + Eq + Clone,
    V: Clone,
    Collection: KeyValueCollection<K, Option<V>>,
{
    fn into(self) -> TxLogEntry<KeyValueRecord<K, V, Collection>> {
        let mut local_collection = self.local_collection;
        let mut total_num_added = self.num_added;
        let mut total_num_removed = self.num_removed;
        let mut new_prev = None;
        if let Some(prev) = self.prev {
            let mut num_iter = 0;
            let mut expected_additional_capacity = 0;

            prev.apply_on_list(|prev| match prev.record() {
                KeyValueRecord::Segment {
                    collection,
                    num_added: _num_added,
                    num_removed: _num_removed,
                    _phantom_key,
                    _phantom_val,
                } => {
                    if collection.len()
                        >= (local_collection.len() + expected_additional_capacity) * 2
                    {
                        return false;
                    }
                    num_iter += 1;
                    expected_additional_capacity += collection.len();
                    true
                }
            });
            local_collection.reserve(expected_additional_capacity);

            prev.apply_on_list(|prev| {
                match prev.record() {
                    KeyValueRecord::Segment {
                        collection,
                        num_added,
                        num_removed,
                        _phantom_key,
                        _phantom_val,
                    } => {
                        if num_iter == 0 || collection.len() >= local_collection.len() * 2 {
                            new_prev = Some(<Arc<_> as Clone>::clone(prev));
                            return false;
                        }
                        num_iter -= 1;

                        collection.iter_key_values(|key, val| {
                            if let Some(None) = local_collection.get(key) {
                                local_collection.remove(key);
                            } else {
                                local_collection.insert(key.clone(), val.clone());
                            }
                        });

                        let (prev_added, prev_removed) =
                            normalize_add_remove(*num_added, *num_removed);
                        let (cur_added, cur_removed) =
                            normalize_add_remove(total_num_added, total_num_removed);
                        total_num_added = prev_added + cur_added;
                        total_num_removed = cur_removed + prev_removed;
                    }
                }
                true
            });
        }

        if new_prev.is_none() {
            let mut new_collection = Collection::with_capacity(local_collection.len());
            local_collection.iter_key_values(|key, value| {
                if value.is_some() {
                    new_collection.insert(key.clone(), value.clone());
                }
            });
            local_collection = new_collection;
        }
        local_collection.shrink_to_fit();

        let (total_num_added, total_num_removed) =
            normalize_add_remove(total_num_added, total_num_removed);

        TxLogEntry::new(
            KeyValueRecord::Segment {
                collection: local_collection,
                num_added: total_num_added,
                num_removed: total_num_removed,
                _phantom_key: PhantomData,
                _phantom_val: PhantomData,
            },
            new_prev,
        )
    }
}

impl<K, V, Collection> Default for KeyValueView<K, V, Collection>
where
    Collection: KeyValueCollection<K, Option<V>>,
{
    fn default() -> Self {
        Self {
            local_collection: Collection::default(),
            prev: None,
            num_added: 0,
            num_removed: 0,
        }
    }
}

impl<K, V, Collection> KeyValueCollection<K, V> for KeyValueView<K, V, Collection>
where
    K: Hash + Eq + Clone,
    V: Clone,
    Collection: KeyValueCollection<K, Option<V>>,
{
    fn insert(&mut self, key: K, val: V) -> Option<V> {
        if let Some(old) = self.local_collection.remove(&key) {
            self.local_collection.insert(key, Some(val));
            if let Some(old) = old {
                // overwrote local collection; no need to increment
                Some(old)
            } else {
                // overwrote removal; need to decrement
                debug_assert!(self.num_removed > 0);
                self.num_removed -= 1;
                None
            }
        } else {
            let mut add_removed = false; // for overwritten value
            self.apply_on_prevs(|prev| {
                match prev.record() {
                    KeyValueRecord::Segment {
                        collection,
                        num_added: _num_added,
                        num_removed: _num_removed,
                        _phantom_key,
                        _phantom_val,
                    } => {
                        if let Some(val) = collection.get(&key) {
                            if val.is_some() {
                                // overwrote old value
                                add_removed = true;
                            } else {
                                // overwrote removal
                            }
                            false
                        } else {
                            // continue to next
                            true
                        }
                    }
                }
            }); // if not found, new key seen
            self.local_collection.insert(key, Some(val));
            if add_removed {
                self.num_removed += 1;
            }
            self.num_added += 1;
            None
        }
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if self.local_collection.contains(key) {
            if let Some(old) = self.local_collection.insert(key.clone(), None).unwrap() {
                debug_assert!(self.num_added > 0);
                self.num_added -= 1;
                Some(old)
            } else {
                None
            }
        } else {
            self.local_collection.insert(key.clone(), None);
            let mut rv = None;
            self.apply_on_prevs(|prev| match prev.record() {
                KeyValueRecord::Segment {
                    collection,
                    num_added: _num_added,
                    num_removed: _num_removed,
                    _phantom_key,
                    _phantom_val,
                } => {
                    if let Some(val) = collection.get(key) {
                        debug_assert!(val.is_some());
                        rv = val.clone();
                        false
                    } else {
                        true
                    }
                }
            });
            if rv.is_some() {
                self.num_removed += 1;
            }
            rv
        }
    }

    fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        if let Some(val) = self.local_collection.get(key) {
            val.as_ref()
        } else {
            let mut rv = None;
            self.apply_on_prevs(|prev| match &prev.record {
                KeyValueRecord::Segment {
                    collection,
                    num_added: _num_added,
                    num_removed: _num_removed,
                    _phantom_key,
                    _phantom_val,
                } => {
                    if let Some(val) = collection.get(key) {
                        rv = val.as_ref();
                        false
                    } else {
                        true
                    }
                }
            });
            rv
        }
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.local_collection.contains(key) {
            return self.local_collection.get_mut(key).unwrap().as_mut();
        }

        let mut rv = None;
        self.apply_on_prevs(|prev| match &prev.record {
            KeyValueRecord::Segment {
                collection,
                num_added: _num_added,
                num_removed: _num_removed,
                _phantom_key,
                _phantom_val,
            } => {
                if let Some(val) = collection.get(key) {
                    rv = val.clone();
                    false
                } else {
                    false
                }
            }
        });

        if let Some(rv) = rv {
            self.num_added += 1;
            self.local_collection.insert(key.clone(), Some(rv));
            self.local_collection.get_mut(key).unwrap().as_mut()
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        let mut cur_num_added = self.num_added;
        let mut cur_num_removed = self.num_removed;
        let mut visited = 0;
        self.apply_on_prevs(|prev| match prev.record() {
            KeyValueRecord::Segment {
                collection: _collection,
                num_added,
                num_removed,
                _phantom_key,
                _phantom_val,
            } => {
                let (new_num_added, new_num_removed) =
                    normalize_add_remove(cur_num_added + num_added, cur_num_removed + num_removed);
                cur_num_added = new_num_added;
                cur_num_removed = new_num_removed;
                visited += 1;
                true
            }
        });

        debug_assert!(cur_num_added >= cur_num_removed);
        cur_num_added - cur_num_removed
    }

    fn iter_key_values<F>(&self, mut iter_fn: F)
    where
        F: FnMut(&K, &V),
    {
        let mut visited = HashSet::new();

        self.local_collection
            .iter_key_values(|key, val| Self::visit(key, val, &mut visited, &mut iter_fn));

        self.apply_on_prevs(|prev| match prev.record() {
            KeyValueRecord::Segment {
                collection,
                num_added: _num_added,
                num_removed: _num_removed,
                _phantom_key,
                _phantom_val,
            } => {
                collection
                    .iter_key_values(|key, val| Self::visit(key, val, &mut visited, &mut iter_fn));
                true
            }
        });
    }

    fn with_capacity(_size: usize) -> Self {
        Self::default() // not supported
    }

    fn reserve(&mut self, _size: usize) {
        // noop; not supported
    }

    fn shrink_to_fit(&mut self) {
        // noop; not supported
    }
}

#[derive(Debug)]
pub struct TxHashMap<K, V> {
    inner: HashMap<K, V>,
}

impl<K, V> KeyValueCollection<K, V> for TxHashMap<K, V>
where
    K: Hash + Eq,
{
    fn insert(&mut self, key: K, val: V) -> Option<V> {
        self.inner.insert(key, val)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.inner.get(key)
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn iter_key_values<F: FnMut(&K, &V)>(&self, mut iter_fn: F) {
        self.inner.iter().for_each(|(key, val)| iter_fn(key, val))
    }

    fn with_capacity(size: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(size),
        }
    }

    fn reserve(&mut self, size: usize) {
        self.inner.reserve(size);
    }

    fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit();
    }
}

impl<K, V> Default for TxHashMap<K, V> {
    fn default() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

type TxLogHmRecord<K, V> = KeyValueRecord<K, V, TxHashMap<K, Option<V>>>;
type TxLogHmHead<K, V> = TxLogHead<TxLogHmRecord<K, V>>;
type TxLogHmView<K, V> = KeyValueView<K, V, TxHashMap<K, Option<V>>>;
type TxNbLogHmContainer<K, V> = TxNonblockingContainer<TxLogHmHead<K, V>>;
type TxNbLogHmHandle<K, V> =
    TxLogStructureHandle<TxLogHmRecord<K, V>, TxNbLogHmContainer<K, V>, TxLogHmView<K, V>>;

#[derive(Debug)]
pub struct TxNonblockingLogHashMap<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    log_structure: TxLogStructure<TxLogHmRecord<K, V>, TxNbLogHmContainer<K, V>>,
}

impl<K, V> TxNonblockingLogHashMap<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            log_structure: TxLogStructure::new(),
        }
    }

    pub fn handle(&self) -> TxNbLogHmHandle<K, V> {
        self.log_structure.handle()
    }

    pub fn handle_for_guard(&self, commit_guard: CommitGuard) -> TxNbLogHmHandle<K, V> {
        self.log_structure.handle_for_guard(commit_guard)
    }
}

impl<K, V> Default for TxNonblockingLogHashMap<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

type TxBLogHmContainer<K, V> = TxBlockingContainer<TxLogHmHead<K, V>>;
type TxBLogHmHandle<K, V> =
    TxLogStructureHandle<TxLogHmRecord<K, V>, TxBLogHmContainer<K, V>, TxLogHmView<K, V>>;

#[derive(Debug)]
pub struct TxBlockingLogHashMap<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    log_structure: TxLogStructure<TxLogHmRecord<K, V>, TxBLogHmContainer<K, V>>,
}

impl<K, V> TxBlockingLogHashMap<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            log_structure: TxLogStructure::new(),
        }
    }

    pub fn handle(&self) -> TxBLogHmHandle<K, V> {
        self.log_structure.handle()
    }

    pub fn handle_for_guard(&self, commit_guard: CommitGuard) -> TxBLogHmHandle<K, V> {
        self.log_structure.handle_for_guard(commit_guard)
    }
}

impl<K, V> Default for TxBlockingLogHashMap<K, V>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
