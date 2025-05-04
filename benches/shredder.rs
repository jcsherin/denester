use criterion::{black_box, criterion_group, criterion_main, Criterion};
use denester::schema::{bool, integer, string};
use denester::{Schema, SchemaBuilder, Value, ValueBuilder, ValueParser};

fn setup_flat_schema() -> (Schema, Value) {
    let schema = SchemaBuilder::new("flat")
        .field(string("name"))
        .field(integer("id"))
        .field(bool("active"))
        .build();

    let value = ValueBuilder::default()
        .field("name", "User")
        .field("id", 12345)
        .field("active", true)
        .build();

    (schema, value)
}

fn benchmark_flat_schema(c: &mut Criterion) {
    let (schema, value) = setup_flat_schema();

    c.bench_function("flat_schema_shredder", |b| {
        b.iter(|| {
            let parser = ValueParser::new(black_box(&schema), black_box(value.iter_depth_first()));
            for column_value in parser {
                let _ = black_box(column_value);
            }
        })
    });
}

criterion_group!(benchmark_shredder, benchmark_flat_schema);
criterion_main!(benchmark_shredder);
