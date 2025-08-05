use criterion::{black_box, criterion_group, criterion_main, Criterion};
use denester::schema::{bool, integer, optional_string, repeated_group, string};
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

fn setup_nested_schema() -> (Schema, Vec<Value>) {
    let schema = SchemaBuilder::new("Contact")
        .field(optional_string("name"))
        .field(repeated_group(
            "phones",
            vec![optional_string("number"), optional_string("phone_type")],
        ))
        .build();

    let values: Vec<Value> = vec![
        // Alice: has a name and two phones
        ValueBuilder::default()
            .field("name", "Alice")
            .repeated(
                "phones",
                vec![
                    ValueBuilder::default()
                        .field("number", "555-1234")
                        .field("phone_type", "Home")
                        .build(),
                    ValueBuilder::default()
                        .field("number", "555-5678")
                        .field("phone_type", "Work")
                        .build(),
                ],
            )
            .build(),
        // Bob: has only a name
        ValueBuilder::default().field("name", "Bob").build(),
        // Charlie: has a name and an empty list of phones
        ValueBuilder::default()
            .field("name", "Charlie")
            .repeated("phones", Vec::<Value>::new())
            .build(),
        // Diana: has a name and one phone
        ValueBuilder::default()
            .field("name", "Diana")
            .repeated(
                "phones",
                vec![ValueBuilder::default()
                    .field("number", "555-9999")
                    .field("phone_type", "Work")
                    .build()],
            )
            .build(),
        // _: has a phone but no name
        ValueBuilder::default()
            .repeated(
                "phones",
                vec![ValueBuilder::default()
                    .field("phone_type", "Mobile")
                    .build()],
            )
            .build(),
    ];

    (schema, values)
}

fn benchmark_nested_schema(c: &mut Criterion) {
    let (schema, values) = setup_nested_schema();

    c.bench_function("nested_schema_shredder", |b| {
        b.iter(|| {
            for value in values.iter() {
                let parser =
                    ValueParser::new(black_box(&schema), black_box(value.iter_depth_first()));
                for column_value in parser {
                    let _ = black_box(column_value);
                }
            }
        })
    });
}

criterion_group!(
    benchmark_shredder,
    benchmark_flat_schema,
    benchmark_nested_schema
);
criterion_main!(benchmark_shredder);
