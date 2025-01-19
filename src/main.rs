mod record;
use crate::record::{DataType, Field, Schema, SchemaBuilder};

fn main() {
    let s1 = flat_schema();

    println!("Schema -> {}", s1.name());
    // TODO: add a nested schema formatter
    for (index, field) in s1.fields().iter().enumerate() {
        println!(
            "\t[{}] name: {} type: {:?} optional: {}",
            index,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }
}

fn flat_schema() -> Schema {
    let name = Field::new("name", DataType::String, false);
    let age = Field::new("age", DataType::Integer, false);
    let verified = Field::new("verified", DataType::Boolean, true);
    let groups = Field::new("groups", DataType::List(Box::new(DataType::Integer)), true);
    let account = Field::new(
        "account",
        DataType::Struct(vec![name, age, verified]),
        false,
    );

    SchemaBuilder::new("user", vec![])
        .field(account)
        .field(groups)
        .build()
}
