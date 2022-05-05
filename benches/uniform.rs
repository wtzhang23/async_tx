use async_tx::bench::{uniform_sync, uniform_tx};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

pub fn bench_uniform(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("uniform");
        for half_num_threads in 1..=8 {
            group.throughput(Throughput::Elements(half_num_threads));
            group.bench_with_input(
                BenchmarkId::new("sync", half_num_threads * 2),
                &half_num_threads,
                |b, half_num_threads| {
                    b.iter(|| uniform_sync(*half_num_threads as usize * 2 as usize));
                },
            );
            group.bench_with_input(
                BenchmarkId::new("tx", half_num_threads * 2),
                &half_num_threads,
                |b, half_num_threads| {
                    b.iter(|| uniform_tx(*half_num_threads as usize * 2 as usize));
                },
            );
        }
    }
}

criterion_group!(benches, bench_uniform);
criterion_main!(benches);
