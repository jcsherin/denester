use denester::schema::{repeated_group, string};
use denester::{SchemaBuilder, Value, ValueBuilder, ValueParser};

fn main() {
    let schema = SchemaBuilder::new("Contact")
        .field(string("name"))
        .field(repeated_group(
            "phones",
            vec![string("number"), string("phone_type")],
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
        // Eve: has phones but no name
        ValueBuilder::default()
            .repeated(
                "phones",
                vec![ValueBuilder::default()
                    .field("phone_type", "Mobile")
                    .build()],
            )
            .build(),
    ];

    // To parse multiple root values, we iterate through them and create a new parser for each.
    for value in values {
        println!("--- New Record ---");
        let parser = ValueParser::new(&schema, value.iter_depth_first());
        for column in parser {
            println!("{:#?}", column);
        }
    }
}
