use async_tx::async_tx;
use async_tx::data::containers::{TxBlockingContainer, TxDataContainer, TxNonblockingContainer};
use async_tx::data::TxData;
use async_tx::runtime::block_on;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{sync::Arc, thread::spawn};

fn test_counter<C>(num_threads: usize, num_add: usize)
where
    C: TxDataContainer<DataType = usize> + 'static + Send + Sync,
{
    let counter = Arc::new(TxData::<C>::new(0));
    let mut guards = Vec::new();
    let alive_ctr = Arc::new(AtomicUsize::new(num_threads));
    for thread_idx in 0..num_threads {
        let counter = counter.clone();
        let alive_ctr = alive_ctr.clone();
        let guard = spawn(move || {
            block_on(async move {
                for add_num in 0..num_add {
                    async_tx!(
                        repeat | counter | {
                            let prev = *counter.read().await;
                            counter.set(prev + 1);
                        }
                    )
                    .await
                    .unwrap();
                    println!("Thread {thread_idx} finished for addition {add_num}");
                }
            });
            alive_ctr.fetch_sub(1, Ordering::Relaxed);
        });
        guards.push(guard);
    }

    // contester that polls on value
    while alive_ctr.load(Ordering::Relaxed) > 0 {
        let val = block_on(async_tx!(
            repeat | counter | {
                let val = *counter.read().await;
                assert!(!counter.write_pending());
                val
            }
        ))
        .unwrap();
        println!("Main thread read {val}");
    }

    for guard in guards {
        guard.join().unwrap();
    }

    let val = block_on(async_tx!(repeat | counter | { *counter.read().await })).unwrap();
    assert_eq!(val, num_threads * num_add);
}

#[test]
fn nonblocking_counter() {
    test_counter::<TxNonblockingContainer<usize>>(1, 1);
    test_counter::<TxNonblockingContainer<usize>>(1, 100);
    test_counter::<TxNonblockingContainer<usize>>(2, 1);
    test_counter::<TxNonblockingContainer<usize>>(2, 100);
    test_counter::<TxNonblockingContainer<usize>>(4, 100);
    test_counter::<TxNonblockingContainer<usize>>(8, 100);
    test_counter::<TxNonblockingContainer<usize>>(16, 100);
}

#[test]
fn blocking_counter() {
    test_counter::<TxBlockingContainer<usize>>(1, 100);
    test_counter::<TxBlockingContainer<usize>>(1, 1);
    test_counter::<TxBlockingContainer<usize>>(2, 1);
    test_counter::<TxBlockingContainer<usize>>(2, 100);
    test_counter::<TxBlockingContainer<usize>>(4, 100);
    test_counter::<TxBlockingContainer<usize>>(8, 100);
    test_counter::<TxBlockingContainer<usize>>(16, 100);
}
