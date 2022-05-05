use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::{async_tx, wait, CommitGuard, TxNonblockingData};

const BUFFER_SIZE: usize = 32;
const NUM_ENQUEUE_DEQUEUE: usize = 2 * 3 * 10 * 14 * 16;

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

    let num_enqueue_dequeue = NUM_ENQUEUE_DEQUEUE / (num_threads / 2);
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

    let num_enqueue_dequeue = NUM_ENQUEUE_DEQUEUE / (num_threads / 2);
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

#[cfg(test)]
mod tests {
    use super::{mpmc_sync, mpmc_tx};

    #[test]
    fn run_two_sync() {
        mpmc_sync(2);
    }

    #[test]
    fn run_two_tx() {
        mpmc_tx(2);
    }
}
