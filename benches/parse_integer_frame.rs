use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use diy_redis::frame::parse;
use std::io::Cursor;

fn bench_parse_integer(c: &mut Criterion) {
    c.bench_function("parse_integer", |b| {
        b.iter(|| {
            let data = Bytes::from_static(b":12345\r\n");
            let mut cursor = Cursor::new(data.as_ref());
            cursor.set_position(0);
            let result = parse(cursor).unwrap();
            black_box(result);
        })
    });

    let mut group = c.benchmark_group("parse_integer_edge_cases");

    group.bench_function("parse_large_integer", |b| {
        b.iter(|| {
            let data = Bytes::from_static(b":9223372036854775807\r\n");
            let mut cursor = Cursor::new(data.as_ref());
            cursor.set_position(0);
            let result = parse(cursor).unwrap();
            black_box(result);
        })
    });

    group.bench_function("parse_negative_integer", |b| {
        b.iter(|| {
            let data = Bytes::from_static(b":-98765\r\n");
            let mut cursor = Cursor::new(data.as_ref());
            cursor.set_position(0);
            let result = parse(cursor).unwrap();
            black_box(result);
        })
    });

    group.bench_function("parse_invalid_integer", |b| {
        b.iter(|| {
            let data = Bytes::from_static(b":12a3\r\n");
            let mut cursor = Cursor::new(data.as_ref());
            cursor.set_position(0);
            let _ = parse(cursor);
            black_box(());
        })
    });

    group.bench_function("parse_overflowing_integer", |b| {
        b.iter(|| {
            let data = Bytes::from_static(b":9999999999999999999999999999\r\n");
            let mut cursor = Cursor::new(data.as_ref());
            cursor.set_position(0);
            let _ = parse(cursor);
            black_box(());
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(1000);
    targets = bench_parse_integer
}
criterion_main!(benches);
