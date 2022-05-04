use std::sync::Arc;

use async_std::task::block_on;

use crate::async_tx;
use crate::log::collections::KeyValueCollection;

use super::TxNonblockingLogHashMap;

#[test]
fn test_batch_insert_remove() {
    const NUM_INSERTS: usize = 1000;
    const NUM_RETRIES: usize = 10;

    let mut keys = Vec::new();
    for i in 0..NUM_INSERTS {
        keys.push(Arc::new(format!("hello{i}")));
    }
    let keys = Arc::new(keys);

    let map = TxNonblockingLogHashMap::new();
    block_on(async {
        for _ in 0..NUM_RETRIES {
            for i in 0..NUM_INSERTS {
                let keys = keys.clone();

                async_tx!(
                    repeat | map;keys | {
                        let write_view = map.write().await;
                        write_view.insert(keys[i].clone(), Arc::new(format!("world{i}")));
                    }
                )
                .await
                .unwrap();

                let (num_elems, log_size, found_key) = async_tx!(
                    repeat | map;keys | {
                        let read_view = map.read().await;
                        (read_view.len(), read_view.log_size(), read_view.contains(&keys[i]))
                    }
                )
                .await
                .unwrap();

                assert_eq!(num_elems, i + 1);
                assert!(
                    log_size <= (num_elems as f64).log2() as usize + 1,
                    "log size {log_size} too big for {num_elems} elements"
                );
                assert!(found_key);
            }

            for i in 0..NUM_INSERTS {
                let keys = keys.clone();

                let val = async_tx!(
                    repeat | map;keys | {
                        let read_view = map.read().await;
                        read_view.get(&keys[i]).cloned()
                    }
                )
                .await
                .unwrap();
                assert_eq!(&*val.unwrap(), &format!("world{i}"));
            }

            let num_keys_iter = async_tx!(
                repeat | map | {
                    let mut num_keys_iter = 0;
                    let read_view = map.read().await;
                    read_view.iter_key_values(|_key, _val| {
                        num_keys_iter += 1;
                    });
                    num_keys_iter
                }
            )
            .await
            .unwrap();

            assert_eq!(num_keys_iter, NUM_INSERTS);

            for i in 0..NUM_INSERTS {
                let keys = keys.clone();

                async_tx!(
                    repeat | map;keys | {
                        let write_view = map.write().await;
                        write_view.remove(&keys[i]);
                    }
                )
                .await
                .unwrap();

                let (num_elems, _log_size, found_key) = async_tx!(
                    repeat | map;keys | {
                        let read_view = map.read().await;
                        (read_view.len(), read_view.log_size(), read_view.contains(&keys[i]))
                    }
                )
                .await
                .unwrap();

                assert_eq!(num_elems, NUM_INSERTS - i - 1);
                // TODO: verify log size
                assert!(!found_key)
            }

            let num_keys_iter = async_tx!(
                repeat | map | {
                    let mut num_keys_iter = 0;
                    let read_view = map.read().await;
                    read_view.iter_key_values(|_key, _val| {
                        num_keys_iter += 1;
                    });
                    num_keys_iter
                }
            )
            .await
            .unwrap();

            assert_eq!(num_keys_iter, 0);
        }
    });
}
