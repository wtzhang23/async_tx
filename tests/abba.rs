use async_tx::data::containers::{TxBlockingContainer, TxDataContainer, TxNonblockingContainer};
use async_tx::data::TxData;
use async_tx::{abort, async_tx, wait};
use futures::executor::LocalPool;
use std::{sync::Arc, thread::spawn};

fn test_abba<C>(num_swap: usize, need_wait: bool)
where
    C: TxDataContainer<DataType = Option<usize>> + 'static + Send + Sync,
{
    let a = Arc::new(TxData::<C>::new(Some(0)));
    let b = Arc::new(TxData::<C>::new(None));

    // spawn ab
    let ab = {
        let a = a.clone();
        let b = b.clone();
        spawn(move || {
            let mut local_pool = LocalPool::new();
            local_pool.run_until(async move {
                for swap_idx in 0..num_swap {
                    async_tx!(
                        repeat | a,
                        b | {
                            let a_val = a.read().await;
                            let _b_val = b.read().await;

                            if a_val.is_none() {
                                if need_wait {
                                    wait!(a);
                                } else {
                                    abort!();
                                }
                            }
                            let a_val = a_val.clone().unwrap();

                            assert_eq!(a_val, swap_idx * 2);
                            b.set(Some(a_val + 1)).await;
                            a.set(None).await;
                        }
                    )
                    .await
                    .unwrap();
                    println!(
                        "Thread \"ab\" finished moving and incrementing {} to b",
                        swap_idx * 2
                    );
                }
            });
        })
    };
    // spawn ba
    let ba = {
        spawn(move || {
            let mut local_pool = LocalPool::new();
            local_pool.run_until(async move {
                for swap_idx in 0..num_swap {
                    async_tx!(
                        repeat | b,
                        a | {
                            let b_val = b.read().await;
                            let _a_val = a.read().await;
                            if b_val.is_none() {
                                if need_wait {
                                    wait!(b);
                                } else {
                                    abort!();
                                }
                            }
                            let b_val = b_val.clone().unwrap();
                            assert_eq!(b_val, swap_idx * 2 + 1);
                            a.read().await; // trigger lock for a
                            a.set(Some(b_val + 1)).await;
                            b.set(None).await;
                        }
                    )
                    .await
                    .unwrap();
                    println!(
                        "Thread \"ba\" finished moving and incrementing {} to a",
                        swap_idx * 2 + 1
                    );
                }
            });
        })
    };
    ab.join().unwrap();
    ba.join().unwrap();
}

#[test]
fn nonblocking_abba() {
    test_abba::<TxNonblockingContainer<Option<usize>>>(1, false);
    test_abba::<TxNonblockingContainer<Option<usize>>>(1, true);
    test_abba::<TxNonblockingContainer<Option<usize>>>(100, false);
    test_abba::<TxNonblockingContainer<Option<usize>>>(100, true);
}

#[test]
fn blocking_abba() {
    test_abba::<TxBlockingContainer<Option<usize>>>(1, false);
    test_abba::<TxBlockingContainer<Option<usize>>>(1, true);
    test_abba::<TxBlockingContainer<Option<usize>>>(100, false);
    test_abba::<TxBlockingContainer<Option<usize>>>(100, true);
}
