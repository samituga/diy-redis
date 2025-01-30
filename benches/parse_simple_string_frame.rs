use bytes::{Buf, Bytes};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use diy_redis::frame::parse;
use std::io::Cursor;

fn bench_read_line(c: &mut Criterion) {
    c.bench_function("parse_simple_string", |b| {
        b.iter(|| {
            let data = Bytes::from_static(
                b"+Lorem ipsum dolor sit amet, consectetur adipiscing elit. \r\n",
            );
            let mut buff = Cursor::new(data.as_ref());
            let result = parse(&mut buff).unwrap();
            black_box(result);
        })
    });

    let mut group = c.benchmark_group("read_line_edge_cases");

    group.bench_function("parse_simple_string_large_line", |b| {
        b.iter(|| {
            let mut data = Bytes::from_static(b"+")
                .chain(Bytes::from(vec![b'a'; 10_000]))
                .chain(Bytes::from_static(b"\r\n"));
            let data = data.copy_to_bytes(data.remaining());
            let mut buff = Cursor::new(data.chunk());
            let result = parse(&mut buff).unwrap();
            black_box(result);
        })
    });

    group.bench_function("parse_simple_string_incomplete_line", |b| {
        b.iter(|| {
            let data = Bytes::from_static(b"+Partial line without CRLF");
            let mut cursor = Cursor::new(data.as_ref());
            let _ = parse(&mut cursor);
            black_box(());
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(1000);
    targets = bench_read_line
}
criterion_main!(benches);
