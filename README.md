# Denester

- [ ] TODO: Write an introduction.
- [ ] TODO: Link to personal blog on record shredding.

## Development

### Build
```
cargo build
```

### Test
```
cargo test --all-features
```

### Bench
```
cargo bench --bench shredder
```

### Run Example
```
cargo run --example contact
```

## Usage

The high-level APIs to interact with this library are:
* `SchemaBuilder`: To define the schema of the nested data.
* `ValueBuilder`: To create a nested value.
* `ValueParser`: To shred the nested value into columns.

Here is an example of how to use the library to shred a nested data structure.

```rust
use denester::schema::{repeated_group, string};
use denester::{SchemaBuilder, ValueBuilder, ValueParser};

fn main() {
    let schema = SchemaBuilder::new("Contact")
        .field(string("name"))
        .field(repeated_group(
            "phones",
            vec![string("number"), string("phone_type")],
        ))
        .build();

    let value = ValueBuilder::default()
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
        .build();

    let parser = ValueParser::new(&schema, value.iter_depth_first());
    for column in parser {
        println!("{:#?}", column);
    }
}
```

This will produce the following output:

```
Ok(
    StripedColumnValue {
        value: String(
            Some(
                "Alice",
            ),
        ),
        repetition_level: 0,
        definition_level: 0,
    },
)
Ok(
    StripedColumnValue {
        value: String(
            Some(
                "555-1234",
            ),
        ),
        repetition_level: 0,
        definition_level: 1,
    },
)
Ok(
    StripedColumnValue {
        value: String(
            Some(
                "Home",
            ),
        ),
        repetition_level: 0,
        definition_level: 1,
    },
)
Ok(
    StripedColumnValue {
        value: String(
            Some(
                "555-5678",
            ),
        ),
        repetition_level: 1,
        definition_level: 1,
    },
)
Ok(
    StripedColumnValue {
        value: String(
            Some(
                "Work",
            ),
        ),
        repetition_level: 1,
        definition_level: 1,
    },
)
```

## API Ergonomics: `denester` vs. `parquet-rs`

This comparison explores the ergonomic differences between `denester` and the production-grade `parquet-rs` library for processing nested data. While `parquet-rs` is a powerful, flexible, and high-performance library, `denester` proposes an alternative, more streamlined developer experience specifically for the task of *shredding* nested structures.

The core difference lies in the separation of concerns:
- **`denester`**: Separates the creation of a nested value from the process of shredding it. This allows for a more declarative and less error-prone approach.
- **`parquet-rs` (raw API)**: Fuses value creation and shredding into a single, imperative process. The developer is responsible for manually building the columnar (shredded) representation from the beginning.

### 1. Schema Definition

*   **`parquet-rs`:** The schema is defined using `arrow`'s `Schema` and `Field` types. This approach is explicit and gives the developer full control, but it can be verbose.

    ```rust
    // Example from parquet-common/src/contact/arrow.rs
    pub fn get_contact_schema() -> SchemaRef {
        let phone_struct = DataType::Struct(get_contact_phone_fields().into());
        let phones_list_field = Field::new("item", phone_struct, true);

        Arc::new(Schema::new(vec![
            Arc::from(Field::new("name", DataType::Utf8, true)),
            Arc::from(Field::new(
                "phones",
                DataType::List(Arc::new(phones_list_field)),
                true,
            )),
        ]))
    }
    ```

*   **`denester`:** The `SchemaBuilder` provides a more concise, fluent API that more closely mirrors the conceptual model of the data.

    ```rust
    // A comparable schema in `denester`
    let schema = SchemaBuilder::new("Contact")
        .field(string("name"))
        .field(repeated_group(
            "phones",
            vec![string("number"), string("phone_type")],
        ))
        .build();
    ```

### 2. Data Construction and Shredding

This is where the ergonomic contrast is most significant.

*   **`parquet-rs`:** Using the raw `arrow` APIs, the developer does not build a nested struct first. Instead, they build the final columnar arrays directly. This requires manually managing builders for each field, iterating through the data, and correctly appending values and nulls to each builder to represent the nested structure. This process is imperative, complex, and boilerplate-heavy.

    *Actual implementation from `parquet-common/src/contact/arrow.rs`:*
    ```rust
    // This function builds the final columnar arrays directly from the `Contact` structs.
    pub fn create_record_batch(
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
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_option(phone.number());
                    struct_builder
                        .field_builder::<StringDictionaryBuilder<UInt8Type>>(1)
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

*   **`denester`:** The process is declarative and separated into two distinct steps:
    1.  **Value Construction:** Create a nested value using the generic `ValueBuilder`. This code is simple and directly reflects the structure of the data you want to represent.
    2.  **Shredding:** Pass the schema and the created value to the `ValueParser`, which handles the entire shredding process automatically.

    *The complete process in `denester`:*
    ```rust
    // 1. Value Construction
    let value = ValueBuilder::default()
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
        .build();

    // 2. Shredding
    let parser = ValueParser::new(&schema, value.iter_depth_first());
    for column in parser {
        // `column` contains the shredded values with correct
        // definition and repetition levels automatically calculated.
        println!("{:#?}", column);
    }
    ```

### Conclusion

`parquet-rs` and `arrow` provide the powerful, low-level tools necessary for high-performance data processing. However, this power comes at the cost of significant boilerplate and complexity for the common task of shredding nested data.

`denester` offers a higher-level, more ergonomic API focused squarely on this problem. By separating value construction from shredding, it reduces cognitive load, eliminates boilerplate, and provides a more declarative and less error-prone way to flatten nested structures.