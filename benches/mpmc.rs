use async_tx::bench::{mpmc_sync, mpmc_tx};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

pub fn bench_mpmc(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("mpmc");
        for half_num_threads in 1..=8 {
            group.throughput(Throughput::Elements(half_num_threads));
            group.bench_with_input(
                BenchmarkId::new("sync", half_num_threads * 2),
                &half_num_threads,
                |b, half_num_threads| {
                    b.iter(|| mpmc_sync(*half_num_threads as usize * 2 as usize));
                },
            );
            group.bench_with_input(
                BenchmarkId::new("tx", half_num_threads * 2),
                &half_num_threads,
                |b, half_num_threads| {
                    b.iter(|| mpmc_tx(*half_num_threads as usize * 2 as usize));
                },
            );
        }
    }
}

criterion_group!(benches, bench_mpmc);
criterion_main!(benches);
