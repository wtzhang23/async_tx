use async_tx::bench::{hot_cold_sync, hot_cold_tx};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

pub fn bench_uniform(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("hot_cold");
        for half_num_threads in 1..=8 {
            group.throughput(Throughput::Elements(half_num_threads));
            group.bench_with_input(
                BenchmarkId::new("sync", half_num_threads * 2),
                &half_num_threads,
                |b, half_num_threads| {
                    b.iter(|| hot_cold_sync(*half_num_threads as usize * 2 as usize));
                },
            );
            group.bench_with_input(
                BenchmarkId::new("tx", half_num_threads * 2),
                &half_num_threads,
                |b, half_num_threads| {
                    b.iter(|| hot_cold_tx(*half_num_threads as usize * 2 as usize));
                },
            );
        }
    }
}

criterion_group!(benches, bench_uniform);
criterion_main!(benches);
