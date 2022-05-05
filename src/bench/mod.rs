use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::executor::block_on;
use parking_lot::{Condvar, Mutex};
use rand::prelude::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::data::TxDataHandle;
use crate::{async_tx, wait, CommitGuard, TxNonblockingData};

const BUFFER_SIZE: usize = 32;
const NUM_TX: usize = 2 * 3 * 10 * 14 * 16;

pub fn mpmc_sync(num_threads: usize) {
    struct Queue {
        buffer: [u64; BUFFER_SIZE],
        start: usize,
        end: usize,
        is_empty: bool,
    }

    impl Queue {
        pub fn new() -> Self {
            Self {
                buffer: [0; BUFFER_SIZE],
                start: 0,
                end: 0,
                is_empty: true,
            }
        }

        pub fn enqueue(&mut self, val: u64) -> bool {
            if self.start == self.end && !self.is_empty {
                return false;
            }
            self.buffer[self.end] = val;
            self.end = (self.end + 1) % BUFFER_SIZE;
            if self.start == self.end {
                self.is_empty = false;
            }
            true
        }

        pub fn dequeue(&mut self) -> Option<u64> {
            if self.start == self.end && self.is_empty {
                return None;
            }
            let val = self.buffer[self.start];
            self.start = (self.start + 1) % BUFFER_SIZE;
            if self.start == self.end {
                self.is_empty = true;
            }
            Some(val)
        }
    }

    let shared_queue = Arc::new((Mutex::new(Queue::new()), Condvar::new(), Condvar::new()));
    let mut handles = Vec::with_capacity(num_threads);
    let start_barrier = Arc::new((Mutex::new(false), Condvar::new()));

    let sum = Arc::new(AtomicU64::new(0));

    let num_enqueue_dequeue = NUM_TX / (num_threads / 2);
    for thread_idx in 0..num_threads {
        let start_barrier = start_barrier.clone();
        let shared_queue = shared_queue.clone();
        let sum = sum.clone();
        handles.push(std::thread::spawn(move || {
            {
                let mut start_barrier_guard = start_barrier.0.lock();
                if !*start_barrier_guard {
                    start_barrier.1.wait(&mut start_barrier_guard);
                }
            }

            let (q_lock, producer_cv, consumer_cv) = &*shared_queue;
            if thread_idx % 2 == 0 {
                // producer
                for _ in 0..num_enqueue_dequeue {
                    let mut guard = q_lock.lock();
                    while !guard.enqueue(1) {
                        producer_cv.wait(&mut guard);
                    }
                    consumer_cv.notify_all();
                }
            } else {
                // consumer
                let mut acc = 0;
                for _ in 0..num_enqueue_dequeue {
                    let mut guard = q_lock.lock();
                    loop {
                        if let Some(val) = guard.dequeue() {
                            acc += val;
                            break;
                        }
                        consumer_cv.wait(&mut guard);
                    }
                    producer_cv.notify_all();
                }
                sum.fetch_add(acc, Ordering::Relaxed);
            }
        }));
    }

    {
        let mut start_barrier_guard = start_barrier.0.lock();
        *start_barrier_guard = true;
        start_barrier.1.notify_all();
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());

    debug_assert_eq!(
        sum.load(Ordering::Relaxed) as usize,
        num_enqueue_dequeue * num_threads / 2
    );
}

pub fn mpmc_tx(num_threads: usize) {
    #[derive(Clone)]
    struct Queue {
        buffer: Arc<Vec<TxNonblockingData<u64>>>,
        start: usize,
        end: usize,
        is_empty: bool,
    }

    impl Queue {
        pub fn new() -> Self {
            let mut buffer = Vec::new();
            for _ in 0..BUFFER_SIZE {
                buffer.push(TxNonblockingData::new(0));
            }
            let buffer = Arc::new(buffer);

            Self {
                buffer,
                start: 0,
                end: 0,
                is_empty: true,
            }
        }

        pub fn full(&self) -> bool {
            self.start == self.end && !self.is_empty
        }

        pub fn empty(&self) -> bool {
            self.start == self.end && self.is_empty
        }

        pub async fn enqueue(&mut self, val: u64, commit_guard: CommitGuard) -> bool {
            if self.start == self.end && !self.is_empty {
                return false;
            }
            self.buffer[self.end]
                .handle_for_guard(commit_guard)
                .set(val)
                .await;
            self.end = (self.end + 1) % BUFFER_SIZE;
            if self.start == self.end {
                self.is_empty = false;
            }
            true
        }

        pub async fn dequeue(&mut self, commit_guard: CommitGuard) -> Option<u64> {
            if self.start == self.end && self.is_empty {
                return None;
            }
            let val = *self.buffer[self.start]
                .handle_for_guard(commit_guard)
                .read()
                .await;
            self.start = (self.start + 1) % BUFFER_SIZE;
            if self.start == self.end {
                self.is_empty = true;
            }
            Some(val)
        }
    }

    let shared_queue = TxNonblockingData::new(Queue::new());
    let mut handles = Vec::with_capacity(num_threads);
    let start_barrier = Arc::new((Mutex::new(false), Condvar::new()));

    let sum = Arc::new(AtomicU64::new(0));

    let num_enqueue_dequeue = NUM_TX / (num_threads / 2);
    for thread_idx in 0..num_threads {
        let start_barrier = start_barrier.clone();
        let shared_queue = shared_queue.clone();
        let sum = sum.clone();
        handles.push(std::thread::spawn(move || {
            {
                let mut start_barrier_guard = start_barrier.0.lock();
                if !*start_barrier_guard {
                    start_barrier.1.wait(&mut start_barrier_guard);
                }
            }

            let commit_guard = CommitGuard::new();

            if thread_idx % 2 == 0 {
                // producer
                for _ in 0..num_enqueue_dequeue {
                    futures::executor::block_on(async_tx!(
                        repeat | shared_queue | {
                            if shared_queue.read().await.full() {
                                wait!(shared_queue);
                            }
                            shared_queue.write().await.enqueue(1, commit_guard).await
                        }
                    ))
                    .unwrap();
                }
            } else {
                // consumer
                let mut acc = 0;
                for _ in 0..num_enqueue_dequeue {
                    let val = futures::executor::block_on(async_tx!(
                        repeat | shared_queue | {
                            if shared_queue.read().await.empty() {
                                wait!(shared_queue);
                            }
                            shared_queue
                                .write()
                                .await
                                .dequeue(commit_guard)
                                .await
                                .unwrap()
                        }
                    ))
                    .unwrap();
                    acc += val;
                }
                sum.fetch_add(acc, Ordering::Relaxed);
            }
        }));
    }

    {
        let mut start_barrier_guard = start_barrier.0.lock();
        *start_barrier_guard = true;
        start_barrier.1.notify_all();
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());

    debug_assert_eq!(
        sum.load(Ordering::Relaxed) as usize,
        num_enqueue_dequeue * num_threads / 2
    );
}

const MAP_SZ: usize = 100;
const TAKE_COUNT: usize = 5;
const HOT_PERCENTAGE: f64 = 0.9;
const NUM_HOT: usize = 10;

pub fn uniform_sync(num_threads: usize) {
    let mut shared_map = HashMap::new();
    let mut keys = Vec::new();
    for i in 0..MAP_SZ {
        shared_map.insert(i, Mutex::new(1));
        keys.push(i);
    }
    let shared_map = Arc::new(shared_map);
    let keys = Arc::new(keys);

    let mut handles = Vec::with_capacity(num_threads);
    let start_barrier = Arc::new((Mutex::new(false), Condvar::new()));

    for thread_idx in 0..num_threads {
        let start_barrier = start_barrier.clone();
        let shared_map = shared_map.clone();
        let keys = keys.clone();
        handles.push(std::thread::spawn(move || {
            {
                let mut start_barrier_guard = start_barrier.0.lock();
                if !*start_barrier_guard {
                    start_barrier.1.wait(&mut start_barrier_guard);
                }
            }

            let mut rand = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(thread_idx as u64);
            for _ in 0..NUM_TX {
                let mut to_lock: Vec<_> = keys
                    .as_slice()
                    .choose_multiple(&mut rand, TAKE_COUNT)
                    .cloned()
                    .collect();
                let to_set = to_lock[0];
                to_lock.sort_unstable();
                to_lock.dedup();
                let write_idx = to_lock
                    .iter()
                    .cloned()
                    .position(|key| key == to_set)
                    .unwrap();
                let mut locks: Vec<_> = to_lock
                    .iter()
                    .cloned()
                    .map(|key| shared_map.get(&key).unwrap().lock())
                    .collect();
                let mut sum = 0;
                for lock in &locks {
                    let val: usize = **lock;
                    sum = val;
                }
                *locks[write_idx] = sum;
            }
        }));
    }

    {
        let mut start_barrier_guard = start_barrier.0.lock();
        *start_barrier_guard = true;
        start_barrier.1.notify_all();
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn uniform_tx(num_threads: usize) {
    let mut shared_map = HashMap::new();
    let mut keys = Vec::new();
    for i in 0..MAP_SZ {
        shared_map.insert(i, TxNonblockingData::new(0));
        keys.push(i);
    }
    let shared_map = Arc::new(shared_map);
    let keys = Arc::new(keys);

    let mut handles = Vec::with_capacity(num_threads);
    let start_barrier = Arc::new((Mutex::new(false), Condvar::new()));

    for thread_idx in 0..num_threads {
        let start_barrier = start_barrier.clone();
        let shared_map = shared_map.clone();
        let keys = keys.clone();
        handles.push(std::thread::spawn(move || {
            {
                let mut start_barrier_guard = start_barrier.0.lock();
                if !*start_barrier_guard {
                    start_barrier.1.wait(&mut start_barrier_guard);
                }
            }

            let mut rand = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(thread_idx as u64);

            block_on(async move {
                for _ in 0..NUM_TX {
                    let mut to_lock: Vec<_> = keys
                        .as_slice()
                        .choose_multiple(&mut rand, TAKE_COUNT)
                        .cloned()
                        .collect();
                    let to_set = to_lock[0];
                    to_lock.sort_unstable();
                    to_lock.dedup();
                    let write_idx = to_lock
                        .iter()
                        .cloned()
                        .position(|key| key == to_set)
                        .unwrap();

                    loop {
                        let mut data_handles: Vec<TxDataHandle<_>> = to_lock
                            .iter()
                            .map(|key| shared_map.get(key).unwrap().handle())
                            .collect();
                        let res = async_tx!({
                            let mut sum = 0;
                            for data_handle in &mut data_handles {
                                sum += *data_handle.read().await;
                            }
                            data_handles[write_idx].set(sum).await;
                        })
                        .await;

                        if let Ok(()) = res {
                            break;
                        }
                    }
                }
            });
        }));
    }

    {
        let mut start_barrier_guard = start_barrier.0.lock();
        *start_barrier_guard = true;
        start_barrier.1.notify_all();
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn hot_cold_sync(num_threads: usize) {
    let mut shared_map = HashMap::new();
    let mut keys = Vec::new();
    for i in 0..MAP_SZ {
        shared_map.insert(i, Mutex::new(1));
        keys.push(i);
    }
    let shared_map = Arc::new(shared_map);

    let mut handles = Vec::with_capacity(num_threads);
    let start_barrier = Arc::new((Mutex::new(false), Condvar::new()));

    for thread_idx in 0..num_threads {
        let start_barrier = start_barrier.clone();
        let shared_map = shared_map.clone();
        handles.push(std::thread::spawn(move || {
            {
                let mut start_barrier_guard = start_barrier.0.lock();
                if !*start_barrier_guard {
                    start_barrier.1.wait(&mut start_barrier_guard);
                }
            }

            let mut rand = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(thread_idx as u64);
            for _ in 0..NUM_TX {
                let mut to_lock = Vec::new();
                for _ in 0..NUM_HOT {
                    if rand.gen::<f64>() < HOT_PERCENTAGE {
                        let hot_idx = rand.gen::<usize>() % NUM_HOT;
                        to_lock.push(hot_idx);
                    } else {
                        let cold_idx = (rand.gen::<usize>() % (MAP_SZ - NUM_HOT)) + NUM_HOT;
                        to_lock.push(cold_idx);
                    }
                }

                let to_set = to_lock[0];
                to_lock.sort_unstable();
                to_lock.dedup();
                let write_idx = to_lock
                    .iter()
                    .cloned()
                    .position(|key| key == to_set)
                    .unwrap();
                let mut locks: Vec<_> = to_lock
                    .iter()
                    .cloned()
                    .map(|key| shared_map.get(&key).unwrap().lock())
                    .collect();
                let mut sum = 0;
                for lock in &locks {
                    let val: usize = **lock;
                    sum = val;
                }
                *locks[write_idx] = sum;
            }
        }));
    }

    {
        let mut start_barrier_guard = start_barrier.0.lock();
        *start_barrier_guard = true;
        start_barrier.1.notify_all();
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

pub fn hot_cold_tx(num_threads: usize) {
    let mut shared_map = HashMap::new();
    let mut keys = Vec::new();
    for i in 0..MAP_SZ {
        shared_map.insert(i, TxNonblockingData::new(0));
        keys.push(i);
    }
    let shared_map = Arc::new(shared_map);

    let mut handles = Vec::with_capacity(num_threads);
    let start_barrier = Arc::new((Mutex::new(false), Condvar::new()));

    for thread_idx in 0..num_threads {
        let start_barrier = start_barrier.clone();
        let shared_map = shared_map.clone();
        handles.push(std::thread::spawn(move || {
            {
                let mut start_barrier_guard = start_barrier.0.lock();
                if !*start_barrier_guard {
                    start_barrier.1.wait(&mut start_barrier_guard);
                }
            }

            let mut rand = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(thread_idx as u64);

            block_on(async move {
                for _ in 0..NUM_TX {
                    let mut to_lock = Vec::new();
                    for _ in 0..NUM_HOT {
                        if rand.gen::<f64>() < HOT_PERCENTAGE {
                            let hot_idx = rand.gen::<usize>() % NUM_HOT;
                            to_lock.push(hot_idx);
                        } else {
                            let cold_idx = (rand.gen::<usize>() % (MAP_SZ - NUM_HOT)) + NUM_HOT;
                            to_lock.push(cold_idx);
                        }
                    }

                    let to_set = to_lock[0];
                    to_lock.sort_unstable();
                    to_lock.dedup();
                    let write_idx = to_lock
                        .iter()
                        .cloned()
                        .position(|key| key == to_set)
                        .unwrap();

                    loop {
                        let mut data_handles: Vec<TxDataHandle<_>> = to_lock
                            .iter()
                            .map(|key| shared_map.get(key).unwrap().handle())
                            .collect();
                        let res = async_tx!({
                            let mut sum = 0;
                            for data_handle in &mut data_handles {
                                sum += *data_handle.read().await;
                            }
                            data_handles[write_idx].set(sum).await;
                        })
                        .await;

                        if let Ok(()) = res {
                            break;
                        }
                    }
                }
            });
        }));
    }

    {
        let mut start_barrier_guard = start_barrier.0.lock();
        *start_barrier_guard = true;
        start_barrier.1.notify_all();
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_two_mpmc_sync() {
        mpmc_sync(2);
    }

    #[test]
    fn run_two_mpmc_tx() {
        mpmc_tx(2);
    }

    #[test]
    fn run_two_uniform_sync() {
        uniform_sync(2);
    }

    #[test]
    fn run_two_uniform_tx() {
        uniform_tx(2);
    }

    #[test]
    fn run_two_hot_cold_sync() {
        hot_cold_sync(2);
    }

    #[test]
    fn run_two_hot_cold_tx() {
        hot_cold_tx(2);
    }
}
