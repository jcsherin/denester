mod record;
use crate::record::{DataType, Field, Schema, SchemaBuilder};

fn main() {
    let s1 = flat_schema();

    println!("Schema -> {}", s1.name());
    for (index, field) in s1.fields().iter().enumerate() {
        println!("\t[{}] {:?}", index, field);
    }
}

fn flat_schema() -> Schema {
    let name = Field::new("name", DataType::String, false);
    let age = Field::new("age", DataType::Integer, false);
    let verified = Field::new("verified", DataType::Boolean, true);

    SchemaBuilder::new("user", vec![name, age])
        .field(verified)
        .build()
}
