use async_tx::data::containers::{TxBlockingContainer, TxDataContainer, TxNonblockingContainer};
use async_tx::data::TxData;
use async_tx::runtime::block_on;
use async_tx::{async_tx, wait};
use std::{sync::Arc, thread::spawn};

fn test_spsc<C>(num_enqueue: usize)
where
    C: TxDataContainer<DataType = Option<usize>> + 'static + Send + Sync,
{
    let queue = Arc::new(TxData::<C>::new(None));
    // spawn producer
    let producer_guard = {
        let queue = queue.clone();
        spawn(move || {
            block_on(async move {
                for to_enqueue in 0..num_enqueue {
                    async_tx!(
                        repeat | queue | {
                            if queue.read().await.is_some() {
                                wait!(queue);
                            }
                            queue.set(Some(to_enqueue));
                        }
                    )
                    .await
                    .unwrap();
                    println!("Producer finished for enqueue number {to_enqueue}");
                }
            });
        })
    };
    // spawn consumer
    let consumer_guard = {
        spawn(move || {
            block_on(async move {
                for to_dequeue in 0..num_enqueue {
                    let dequeued = async_tx!(
                        repeat | queue | {
                            if queue.read().await.is_none() {
                                wait!(queue);
                            }
                            let val = queue.read().await.clone();
                            queue.set(None);
                            val.unwrap()
                        }
                    )
                    .await
                    .unwrap();
                    assert_eq!(dequeued, to_dequeue);
                    println!("Consumer finished for dequeue number {to_dequeue}");
                }
            });
        })
    };
    producer_guard.join().unwrap();
    consumer_guard.join().unwrap();
}

#[test]
fn nonblocking_spsc() {
    test_spsc::<TxNonblockingContainer<Option<usize>>>(1);
    test_spsc::<TxNonblockingContainer<Option<usize>>>(100);
}

#[test]
fn blocking_spsc() {
    test_spsc::<TxBlockingContainer<Option<usize>>>(1);
    test_spsc::<TxBlockingContainer<Option<usize>>>(100);
}
