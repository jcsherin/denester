[![Rust CI](https://github.com/jcsherin/denester/actions/workflows/rust.yml/badge.svg)](https://github.com/jcsherin/denester/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Denester** is a proof-of-concept implementation of the
[Dremel shredding algorithm] for shredding nested data structures. It is a
zero-dependency library with a lightweight type system capable of expressing
nested data structures.

Shredding nested data structures using a library like [arrow-rs] can be complex,
often requiring you to manually manage the state of the column array builders
for every field in the schema. Denester eliminates this complexity by internally
handling value traversal, schema validation, and a state machine for columnar
shredding.

See a detailed [API comparison with arrow-rs](#comparison-with-arrow-shredding-api)

[Dremel shredding algorithm]: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf

[arrow-rs]: https://arrow.apache.org/rust/arrow/array/array/index.html

- [ ] TODO: Link to blog on record shredding.

## Usage

### 1. Create a Schema Definition

```rust
use denester::schema::{optional_string, repeated_group};
use denester::{Schema, SchemaBuilder};

fn contact_schema() -> Schema {
    SchemaBuilder::new("Contact")
        .field(optional_string("name"))
        .field(repeated_group(
            "phones",
            vec![optional_string("number"), optional_string("phone_type")],
        ))
        .build()
}
```

### 2. Create a Nested Data Instance

```rust
use denester::{Value, ValueBuilder};

fn contact_value() -> Value {
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
        .build()
}
```

### 3. Shred into Column Values

```rust
use denester::ValueParser;

fn main() {
    let schema = contact_schema();
    let value = contact_value();

    let iter = ValueParser::new(&schema, value.iter_depth_first());

    println!("--- Shredding New Value ---");
    for shredded_column_value in iter {
        if let Ok(inner) = shredded_column_value {
            println!("{inner}");
        }
    }
}
```

### Expected Output

```text
--- Shredding New Value ---
| String(Some("Alice"))    | name                     | def=1 | rep=0 |
| String(Some("555-1234")) | phones.number            | def=2 | rep=0 |
| String(Some("Home"))     | phones.phone_type        | def=2 | rep=0 |
| String(Some("555-5678")) | phones.number            | def=2 | rep=1 |
| String(Some("Work"))     | phones.phone_type        | def=2 | rep=1 |
```

## Development

### Build

```
cargo build
```

### Run Tests

```
cargo test
```

### Run Benchmarks

```
cargo bench --bench shredder
```

### Run Examples

```

cargo run --example dremel
```

## Comparison with Arrow Shredding API

### 1. Nested Data Definition

```rust
struct Contact {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}

struct Phone {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

enum PhoneType {
    Home,
    Work,
    Mobile,
}
```

### 2. Build the Arrow Schema

```rust
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

fn contact_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // Contact.name
        Arc::from(Field::new("name", DataType::Utf8, true)),

        // Contact.phones
        Arc::from(Field::new(
            "phones",
            DataType::List(Arc::new(
                Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            // Contact.phones.number
                            Arc::from(Field::new("number", DataType::Utf8, true)),
                            // Contact.phones.phone_type
                            Arc::from(Field::new(
                                "phone_type",
                                DataType::Dictionary(
                                    Box::new(DataType::UInt8),
                                    Box::new(DataType::Utf8),
                                ),
                                true,
                            )),
                        ].into(),
                    ),
                    true,
                ),
            )),
            true,
        )),
    ]))
}
```

### 3. Arrow Shredding

```rust
use arrow::array::{
    ListBuilder, RecordBatch, StringBuilder, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{SchemaRef, UInt8Type};
use std::error::Error;
use std::sync::Arc;

pub const PHONE_NUMBER_FIELD_INDEX: usize = 0;
pub const PHONE_TYPE_FIELD_INDEX: usize = 1;

pub fn contacts_to_record_batch(
    schema: SchemaRef,
    contacts: &[Contact],
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut name_builder = StringBuilder::new();

    let phone_number_builder = StringBuilder::new();
    let phone_type_builder = StringDictionaryBuilder::<UInt8Type>::new();
    let phone_struct_builder = StructBuilder::new(
        get_contact_phone_fields(),
        vec![Box::new(phone_number_builder), Box::new(phone_type_builder)],
    );

    let mut phones_list_builder = ListBuilder::new(phone_struct_builder);

    for contact in contacts {
        name_builder.append_option(contact.name());

        if let Some(phones) = contact.phones() {
            let struct_builder = phones_list_builder.values();

            for phone in phones {
                struct_builder.append(true);

                struct_builder
                    .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                    .unwrap()
                    .append_option(phone.number());
                struct_builder
                    .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                    .unwrap()
                    .append_option(phone.phone_type().map(AsRef::as_ref));
            }

            phones_list_builder.append(true);
        } else {
            phones_list_builder.append_null();
        }
    }

    let name_array = Arc::new(name_builder.finish());
    let phones_array = Arc::new(phones_list_builder.finish());

    RecordBatch::try_new(schema, vec![name_array, phones_array]).map_err(Into::into)
}
```

### Denester Shredding

```rust
use denester::schema::{optional_string, repeated_group};
use denester::{Schema, SchemaBuilder, Value, ValueBuilder, ValueParser};

fn contact_schema() -> Schema {
    SchemaBuilder::new("Contact")
        .field(optional_string("name"))
        .field(repeated_group(
            "phones",
            vec![optional_string("number"), optional_string("phone_type")],
        ))
        .build()
}

fn shred_contacts(value: &[Value]) {
    let schema = contact_schema();

    for value in values {
        let parser = ValueParser::new(&schema, value.iter_depth_first());

        parser.into_iter().for_each(|parsed| {
            if let Ok(shredded) = parsed {
                println!("{shredded}");
            }
        })
    }
}

/// Expected Output
/// | String(Some("Alice"))    | name                     | def=1 | rep=0 |
/// | String(Some("555-1234")) | phones.number            | def=2 | rep=0 |
/// | String(Some("Home"))     | phones.phone_type        | def=2 | rep=0 |
/// | String(Some("555-5678")) | phones.number            | def=2 | rep=1 |
/// | String(Some("Work"))     | phones.phone_type        | def=2 | rep=1 |
```

[//]: # (The core difference lies in the separation of concerns:)

[//]: # ()

[//]: # (- **`denester`**: Separates the creation of a nested value from the process of)

[//]: # (  shredding it. This allows for a more declarative and less error-prone)

[//]: # (  approach.)

[//]: # (- **`parquet-rs` &#40;raw API&#41;**: Fuses value creation and shredding into a single,)

[//]: # (  imperative process. The developer is responsible for manually building the)

[//]: # (  columnar &#40;shredded&#41; representation from the beginning.)

[//]: # ()

[//]: # (* **`parquet-rs`:** Using the raw `arrow` APIs, the developer does not build a)

[//]: # (  nested struct first. Instead, they build the final columnar arrays directly.)

[//]: # (  This requires manually managing builders for each field, iterating through the)

[//]: # (  data, and correctly appending values and nulls to each builder to represent)

[//]: # (  the nested structure. This process is imperative, complex, and)

[//]: # (  boilerplate-heavy.)

[//]: # ()

[//]: # (* **`denester`:** The process is declarative and separated into two distinct)

[//]: # (  steps:)

[//]: # (    1. **Value Construction:** Create a nested value using the generic)

[//]: # (       `ValueBuilder`. This code is simple and directly reflects the structure)

[//]: # (       of the data you want to represent.)

[//]: # (    2. **Shredding:** Pass the schema and the created value to the)

[//]: # (       `ValueParser`, which handles the entire shredding process automatically.)
