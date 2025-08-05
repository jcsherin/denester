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

fn main() {
    let schema = contact_schema();

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

    for value in values {
        println!("--- Shredding New Value ---");

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        parser.into_iter().for_each(|parsed| {
            if let Ok(shredded) = parsed {
                println!("{shredded}");
            }
        })
    }
}
