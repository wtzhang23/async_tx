mod future {
    use futures::select;
    use futures::FutureExt;

    use std::future::{pending, ready};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_tx::runtime::block_on;

    #[test]
    fn test_trivial() {
        assert_eq!(block_on(async { 42 }), 42);
    }

    #[test]
    fn test_block_forever() {
        let finished = Arc::new(AtomicBool::new(false));
        {
            let finished = finished.clone();
            std::thread::spawn(move || {
                block_on(async {
                    pending::<()>().await;
                });
                finished.store(true, Ordering::Release);
            });
        }

        std::thread::sleep(Duration::from_millis(500));
        assert!(!finished.load(Ordering::Acquire));
    }

    #[test]
    fn test_select() {
        assert_eq!(
            block_on(async {
                let mut a = pending::<()>().fuse();
                let mut b = ready(42).fuse();
                let mut c = pending::<()>().fuse();
                let mut d = ready(42).fuse();

                select! {
                    _ = a => 0,
                    b_res = b => b_res,
                    _ = c => 0,
                    d_res = d => d_res,
                }
            }),
            42
        );
    }

    #[test]
    fn test_reentry() {
        let a = Arc::new(AtomicBool::new(false));
        let b = Arc::new(AtomicBool::new(false));
        let c = Arc::new(AtomicBool::new(false));
        let d = Arc::new(AtomicBool::new(false));

        block_on(async {
            async_std::task::sleep(Duration::from_millis(10)).await;
            a.store(true, Ordering::Release);
            async_std::task::sleep(Duration::from_millis(20)).await;
            b.store(true, Ordering::Release);
            async_std::task::sleep(Duration::from_millis(30)).await;
            c.store(true, Ordering::Release);
            async_std::task::sleep(Duration::from_millis(40)).await;
            d.store(true, Ordering::Release);
        });

        [a, b, c, d].iter().all(|flag| flag.load(Ordering::Acquire));
    }
}

mod local {
    use futures::select;
    use futures::FutureExt;
    use std::sync::{Condvar, Mutex};

    use std::future::{pending, ready};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_tx::runtime::LocalExecutor;

    #[test]
    fn test_trivial() {
        let mut executor = LocalExecutor::new();

        // no pending tasks
        {
            let val = executor.run_until(async { 42 });
            assert_eq!(val, 42);
        }

        // pending tasks
        {
            let inner_ran = Arc::new(AtomicBool::new(false));
            {
                let inner_ran = inner_ran.clone();
                executor.enqueue(async move {
                    inner_ran.store(true, Ordering::Relaxed);
                });
            }
            let val = executor.run_until(async { 42 });
            assert_eq!(val, 42);
            executor.run_all();
            assert!(inner_ran.load(Ordering::Relaxed));
        }

        // all tasks
        {
            let inner_write = Arc::new(AtomicUsize::new(0));
            {
                let inner_write = inner_write.clone();
                executor.enqueue(async move {
                    inner_write.store(42, Ordering::Relaxed);
                });
            }
            executor.run_all();
            assert_eq!(inner_write.load(Ordering::Relaxed), 42);
        }
    }

    #[test]
    fn test_block_forever() {
        let finished = Arc::new(AtomicBool::new(false));

        // trying to run all futures where one blocks
        {
            let finished = finished.clone();
            std::thread::spawn(move || {
                let mut executor = LocalExecutor::new();
                executor.enqueue(pending::<()>());
                executor.run_all();
                finished.store(true, Ordering::Release);
            });
        }

        // trying to run a future that blocks
        {
            let finished = finished.clone();
            std::thread::spawn(move || {
                let mut executor = LocalExecutor::new();
                executor.run_until(pending::<()>());
                finished.store(true, Ordering::Release);
            });
        }

        std::thread::sleep(Duration::from_millis(500)); // assume deadlock at half a second
        assert!(!finished.load(Ordering::Acquire));
    }

    #[test]
    fn test_run_all_fairness() {
        const NUM_ADD: usize = 100;

        let counter = Arc::new((Mutex::new([false; NUM_ADD]), Condvar::new()));
        {
            let counter = counter.clone();
            std::thread::spawn(move || {
                let mut executor = LocalExecutor::new();
                executor.enqueue(pending::<()>());
                for idx in 0..NUM_ADD {
                    let counter = counter.clone();
                    executor.enqueue(async move {
                        let (mutex, cv) = &*counter;
                        let mut guard = mutex.lock().unwrap();
                        guard[idx] = true;
                        if guard.iter().cloned().all(std::convert::identity) {
                            cv.notify_all();
                        }
                    });
                }
                executor.run_all();
            });
        }

        {
            let (mutex, cv) = &*counter;
            let mut guard = mutex.lock().unwrap();
            while !guard.iter().cloned().all(std::convert::identity) {
                guard = cv.wait(guard).unwrap();
            }
        }
    }

    #[test]
    fn test_run_until_fairness() {
        const NUM_ADD: usize = 100;

        let counter = Arc::new((Mutex::new([false; NUM_ADD]), Condvar::new()));
        {
            let counter = counter.clone();
            std::thread::spawn(move || {
                let mut executor = LocalExecutor::new();
                for idx in 0..NUM_ADD {
                    let counter = counter.clone();
                    executor.enqueue(async move {
                        let (mutex, cv) = &*counter;
                        let mut guard = mutex.lock().unwrap();
                        guard[idx] = true;
                        if guard.iter().cloned().all(std::convert::identity) {
                            cv.notify_all();
                        }
                    });
                }
                executor.run_until(pending::<()>());
            });
        }

        {
            let (mutex, cv) = &*counter;
            let mut guard = mutex.lock().unwrap();
            while !guard.iter().cloned().all(std::convert::identity) {
                guard = cv.wait(guard).unwrap();
            }
        }
    }

    #[test]
    fn test_select() {
        let mut executor = LocalExecutor::new();
        let val = executor.run_until(async {
            let mut a = pending::<()>().fuse();
            let mut b = ready(42).fuse();
            let mut c = pending::<()>().fuse();
            let mut d = ready(42).fuse();

            select! {
                _ = a => 0,
                b_res = b => b_res,
                _ = c => 0,
                d_res = d => d_res,
            }
        });
        assert_eq!(val, 42);

        let inner_val = Arc::new(AtomicUsize::new(0));
        {
            let inner_val = inner_val.clone();
            executor.enqueue(async move {
                let mut a = pending::<()>().fuse();
                let mut b = ready(42).fuse();
                let mut c = pending::<()>().fuse();
                let mut d = ready(42).fuse();

                let val = select! {
                    _ = a => 0,
                    b_res = b => b_res,
                    _ = c => 0,
                    d_res = d => d_res,
                };
                inner_val.store(val, Ordering::Relaxed);
            });
            executor.run_all();
        }

        assert_eq!(inner_val.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn test_reentry() {
        const NUM_ADD: usize = 100;

        let counter = Arc::new(Mutex::new([false; NUM_ADD]));
        let mut executor = LocalExecutor::new();
        for idx in 0..NUM_ADD {
            let counter = counter.clone();
            executor.enqueue(async move {
                let mut guard = counter.lock().unwrap();
                guard[idx] = true;
            });
        }
        executor.run_all();

        {
            let guard = counter.lock().unwrap();
            assert!(guard.iter().cloned().all(std::convert::identity));
        }
    }
}

mod global {
    use async_tx::runtime::{block_on, GlobalExecutor};
    use futures::channel::oneshot::channel;
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_trivial() {
        let executor = GlobalExecutor::new(1);

        let inner_ran = Arc::new(AtomicBool::new(false));
        let (tx, rx) = channel();
        {
            let inner_ran = inner_ran.clone();
            executor.enqueue(async move {
                inner_ran.store(true, Ordering::Relaxed);
                tx.send(()).unwrap();
            });
        }

        block_on(async move {
            rx.await.unwrap();
        });
        assert!(inner_ran.load(Ordering::Relaxed));
    }

    #[test]
    fn test_long_sleep() {
        let executor = GlobalExecutor::new(1);

        let inner_ran = Arc::new(AtomicBool::new(false));
        let (tx, rx) = channel();
        {
            let inner_ran = inner_ran.clone();
            executor.enqueue(async move {
                async_std::task::sleep(Duration::from_millis(100)).await;
                async_std::task::sleep(Duration::from_millis(100)).await;
                async_std::task::sleep(Duration::from_millis(100)).await;
                async_std::task::sleep(Duration::from_millis(100)).await;
                async_std::task::sleep(Duration::from_millis(100)).await;
                inner_ran.store(true, Ordering::Relaxed);
                tx.send(()).unwrap();
            });
        }

        block_on(async move {
            rx.await.unwrap();
        });
        assert!(inner_ran.load(Ordering::Relaxed));
    }

    #[test]
    fn test_fairness() {
        const NUM_ADD: usize = 100;
        let executor = GlobalExecutor::new(4);
        for _ in 0..NUM_ADD {
            executor.enqueue(pending::<()>());
        }

        let inner_ran = Arc::new(AtomicBool::new(false));
        let (tx, rx) = channel();
        {
            let inner_ran = inner_ran.clone();
            executor.enqueue(async move {
                inner_ran.store(true, Ordering::Relaxed);
                tx.send(()).unwrap();
            });
        }

        block_on(async move {
            rx.await.unwrap();
        });
        assert!(inner_ran.load(Ordering::Relaxed));
    }

    #[test]
    fn test_join_all() {
        const NUM_ADD: usize = 100;

        let counter = Arc::new(Mutex::new([false; NUM_ADD]));
        let executor = GlobalExecutor::new(4);
        for idx in 0..NUM_ADD {
            let counter = counter.clone();
            executor.enqueue(async move {
                let mut guard = counter.lock().unwrap();
                guard[idx] = true;
            });
        }
        executor.join_all();

        let guard = counter.lock().unwrap();
        assert!(guard.iter().cloned().all(std::convert::identity));
    }
}
