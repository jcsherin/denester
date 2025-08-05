use denester::parser::StripedColumnValue;
use denester::schema::{
    integer, optional_group, optional_integer, optional_string, repeated_group, repeated_integer,
    required_group, string,
};
use denester::value::Value;
use denester::{DefinitionLevel, RepetitionLevel};
use denester::{SchemaBuilder, ValueBuilder, ValueParser};

// Helper function
fn assert_column_striped_value(
    actual: &StripedColumnValue,
    expected_value: &Value,
    expected_def: DefinitionLevel,
    expected_rep: RepetitionLevel,
    message_prefix: &str,
) {
    assert_eq!(
        actual.value(),
        expected_value,
        "{message_prefix}: Value mismatch",
    );
    assert_eq!(
        actual.definition_level(),
        expected_def,
        "{message_prefix}: Definition level mismatch",
    );
    assert_eq!(
        actual.repetition_level(),
        expected_rep,
        "{message_prefix}: Repetition level mismatch",
    );
}

mod basic_parsing {
    use super::*;

    #[test]
    fn test_nested_struct() {
        let schema = SchemaBuilder::new("nested_struct")
            .field(required_group(
                "a",
                vec![
                    required_group("b", vec![required_group("c", vec![integer("d")])]),
                    optional_group("x", vec![optional_group("y", vec![integer("z")])]),
                ],
            ))
            .build();

        let value = ValueBuilder::default()
            .field(
                "a",
                ValueBuilder::default()
                    .field(
                        "b",
                        ValueBuilder::default()
                            .field("c", ValueBuilder::default().field("d", 1).build())
                            .build(),
                    )
                    .field(
                        "x",
                        ValueBuilder::default()
                            .field("y", ValueBuilder::default().field("z", 2).build())
                            .build(),
                    )
                    .build(),
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 2);

        // Check value d: path a.b.c.d (all required)
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(Some(1)),
            0,
            0,
            "Parsed[0] path a.b.c.d",
        );

        // Check value z: path a.x.y.z (x and y are optional)
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(Some(2)),
            2,
            0,
            "Parsed[1] path a.x.y.z",
        );
    }

    #[test]
    fn test_schema_links() {
        // message doc {
        //   optional group Links {         // def +1
        //      repeated int Backward;      // def +1, rep +1
        //      repeated int Forward; }}    // def +1, rep +1
        let schema = SchemaBuilder::new("Doc")
            .field(optional_group(
                "Links",
                vec![repeated_integer("Backward"), repeated_integer("Forward")],
            ))
            .build();

        // { Links: Forward: [20, 40, 60] } // Backward is missing
        let value = ValueBuilder::default()
            .field(
                "Links",
                ValueBuilder::default()
                    .repeated("Forward", vec![20, 40, 60]) // Missing "backward"
                    .build(),
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected 3 values for Forward, 1 null for missing Backward
        assert_eq!(parsed.len(), 4);

        // Assert present fields first
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(Some(20)),
            2,
            0,
            "Parsed[0] (Forward[0])",
        );
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(Some(40)),
            2,
            1,
            "Parsed[1] (Forward[1])",
        );
        assert_column_striped_value(
            &parsed[2],
            &Value::Integer(Some(60)),
            2,
            1,
            "Parsed[2] (Forward[2])",
        );

        // Assert missing fields
        assert_column_striped_value(
            &parsed[3],
            &Value::Integer(None),
            1,
            0,
            "Parsed[3] (Backward - missing)",
        )
    }

    #[test]
    fn test_schema_code() {
        // message doc {
        //  repeated group Name {           // def +1, rep +1
        //    repeated group Language {     // def +1, rep +1
        //      required string Code; }}}
        let schema = SchemaBuilder::new("doc")
            .field(repeated_group(
                "Name",
                vec![repeated_group("Language", vec![string("Code")])],
            ))
            .build();

        // Name[0]: { Language: [{Code: 'en-us'}, {Code: 'en'}] }
        // Name[1]: {}                                              // Missing language group
        // Name[2]: { Language: [{Code: 'en-gb'}] }
        let value = ValueBuilder::default()
            .repeated(
                "Name",
                vec![
                    // Name[0]
                    ValueBuilder::default()
                        .repeated(
                            "Language",
                            vec![
                                ValueBuilder::default().field("Code", "en-us").build(), // Language[0]
                                ValueBuilder::default().field("Code", "en").build(), // Language[1]
                            ],
                        )
                        .build(),
                    // Name[1] - Empty struct, Language is missing
                    ValueBuilder::default().build(),
                    // Name[2]
                    ValueBuilder::default()
                        .repeated(
                            "Language",
                            vec![ValueBuilder::default().field("Code", "en-gb").build()], // Language[0]
                        )
                        .build(),
                ],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected output
        // Code | r | d
        // -----|---|---
        // en-us| 0 | 2 (First Name, First Language)
        // en   | 2 | 2 (First Name, Second Language)
        // NULL | 1 | 1 (Second Name, Missing Language)
        // en-gb| 1 | 2 (Third Name, First Language)
        assert_eq!(parsed.len(), 4);

        // Name[0].Language[0].Code
        assert_column_striped_value(
            &parsed[0],
            &Value::String(Some(String::from("en-us"))),
            2,
            0,
            "Parsed[0] (Name[0].Language[0].Code)",
        );

        // Name[0].Language[1].Code
        assert_column_striped_value(
            &parsed[1],
            &Value::String(Some(String::from("en"))),
            2,
            2,
            "Parsed[1] (Name[0].Language[1].Code)",
        );
        // Name[1] - Language group is missing
        assert_column_striped_value(
            &parsed[2],
            &Value::String(None),
            1,
            1,
            "Parsed[2] (Name[1] - Language missing)",
        );
        // Name[2].Language[0].Code
        assert_column_striped_value(
            &parsed[3],
            &Value::String(Some(String::from("en-gb"))),
            2,
            1,
            "Parsed[3] (Name[2].Language[0].Code)",
        );
    }
}

mod missing_fields {
    use super::*;
    #[test]
    fn test_nested_missing_fields() {
        // message doc {
        //  required group a {
        //      required int x
        //      optional int y },   // def +1
        //  required int b }
        let schema = SchemaBuilder::new("doc")
            .field(required_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .field(integer("b"))
            .build();

        // { a: { x: 1 }, b: 2 } // y is missing inside a
        let value = ValueBuilder::default()
            .field("a", ValueBuilder::default().field("x", 1).build()) // y is missing
            .field("b", 2)
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        // a.x is present in the value
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(Some(1)),
            0,
            0,
            "Parsed[0] (a.x)",
        );

        // a.y: y is missing, processed after present fields in its group 'a'
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(None),
            0,
            0,
            "Parsed[1] (a.y - missing)",
        );

        // b
        assert_column_striped_value(&parsed[2], &Value::Integer(Some(2)), 0, 0, "Parsed[2] (b)");
    }

    #[test]
    fn test_multiple_nested_missing_fields() {
        // message doc {
        //  required group a {
        //      required int x
        //      optional int y },   // def +1
        //  optional int b }        // def +1
        let schema = SchemaBuilder::new("doc")
            .field(required_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .field(optional_integer("b"))
            .build();

        // { a: { x: 1 } } // y is missing inside a, b is missing at top-level
        let value = ValueBuilder::default()
            .field("a", ValueBuilder::default().field("x", 1).build())
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        // a.x
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(Some(1)),
            0,
            0,
            "Parsed[0] (a.x)",
        );

        // a.y: y is optional and missing
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(None),
            0,
            0,
            "Parsed[1] (a.y - missing)",
        );

        // b: b is optional and missing
        assert_column_striped_value(
            &parsed[2],
            &Value::Integer(None),
            0,
            0,
            "Parsed[2] (b - missing)",
        );
    }

    #[test]
    fn test_missing_struct() {
        // message doc {
        //  optional group a {      // def +1
        //      required int x
        //      optional int y },   // def +1
        //  required int b }
        let schema = SchemaBuilder::new("doc")
            .field(optional_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .field(integer("b"))
            .build();

        // { b: 1 } // Entire group 'a' is missing
        let value = ValueBuilder::default().field("b", 1).build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        // Assert present fields first
        // b
        assert_column_striped_value(&parsed[0], &Value::Integer(Some(1)), 0, 0, "Parsed[0] (b)");

        // Assert missing fields from group 'a'
        // a.x
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(None),
            0,
            0,
            "Parsed[1] (a.x - missing)",
        );

        // a.y
        assert_column_striped_value(
            &parsed[2],
            &Value::Integer(None),
            0,
            0,
            "Parsed[2] (a.y - missing)",
        );
    }

    #[test]
    fn test_deep_nesting_missing_fields() {
        // message doc {
        //  optional group a {                  // def +1
        //      required group b {
        //          optional group c {          // def +1
        //             required int x;
        //             optional int y; }}}}     // def +1
        let schema = SchemaBuilder::new("doc")
            .field(optional_group(
                "a",
                vec![required_group(
                    "b",
                    vec![optional_group(
                        "c",
                        vec![integer("x"), optional_integer("y")],
                    )],
                )],
            ))
            .build();

        // Value: { a: { b: { c: { x: 1 } } } } // y is missing inside c
        let value = ValueBuilder::default()
            .field(
                "a",
                ValueBuilder::default()
                    .field(
                        "b",
                        ValueBuilder::default()
                            .field("c", ValueBuilder::default().field("x", 1).build())
                            .build(),
                    )
                    .build(),
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 2);

        // Assert present field
        // a.b.c.x
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(Some(1)),
            2,
            0,
            "Parsed[0] (a.b.c.x)",
        );

        // Assert missing field
        // a.b.c.y - y is missing inside group 'c'
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(None),
            2,
            0,
            "Parsed[1] (a.b.c.y - missing)",
        );
    }
}

mod repeated_fields {
    use super::*;

    #[test]
    fn test_repeated_struct_with_missing_values() {
        // message doc {
        //  repeated group a {      // def +1, rep +1
        //      required int x;
        //      optional int y; }}  // def +1
        let schema = SchemaBuilder::new("doc")
            .field(repeated_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .build();

        // { a: [{x: 1}, {x: 2, y: 3} // y is missing in the first element of 'a'
        let value = ValueBuilder::default()
            .field(
                "a",
                vec![
                    ValueBuilder::default().field("x", 1).build(), // y is missing here
                    ValueBuilder::default().field("x", 2).field("y", 3).build(),
                ],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 4);

        // --- First struct in list 'a' ---
        // a[0].x
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(Some(1)),
            1,
            0,
            "Parsed[0] (a[0].x)",
        );
        // a[0].y - y is missing
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(None),
            1,
            0,
            "Parsed[1] (a[0].y - missing)",
        );

        // --- Second struct in list 'a' ---
        // a[1].x
        assert_column_striped_value(
            &parsed[2],
            &Value::Integer(Some(2)),
            1,
            1,
            "Parsed[2] (a[1].x)",
        );
        // a[1].y
        assert_column_striped_value(
            &parsed[3],
            &Value::Integer(Some(3)),
            2,
            1,
            "Parsed[3] (a[1].y)",
        );
    }

    #[test]
    fn test_backtrack_struct_siblings() {
        // message doc {
        //  required group a {
        //      optional int x },   // def +1
        //  required group b {
        //      optional int y }}   // def +1
        let schema = SchemaBuilder::new("doc")
            .field(required_group("a", vec![optional_integer("x")]))
            .field(required_group("b", vec![optional_integer("y")]))
            .build();

        // { a: {}, b: {} } // x is missing in a, y is missing in b
        let value = ValueBuilder::default()
            .field("a", ValueBuilder::default().build())
            .field("b", ValueBuilder::default().build())
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 2);

        // Assert missing fields
        // a.x (x is missing)
        assert_column_striped_value(
            &parsed[0],
            &Value::Integer(None),
            0,
            0,
            "Parsed[0] (a.x - missing)",
        );
        // b.y (y is missing)
        assert_column_striped_value(
            &parsed[1],
            &Value::Integer(None),
            0,
            0,
            "Parsed[1] (b.y - missing)",
        );
    }

    #[test]
    fn test_missing_optional_repeated_group() {
        // message Contact {
        //   optional string name;
        //   repeated group phones {
        //     optional string number;
        //     optional string phone_type;
        //   }
        // }
        let schema = SchemaBuilder::new("Contact")
            .field(optional_string("name"))
            .field(repeated_group(
                "phones",
                vec![optional_string("number"), optional_string("phone_type")],
            ))
            .build();

        // { name: "Bob" } // phones group is missing
        let value = ValueBuilder::default().field("name", "Bob").build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected output:
        // | value                    | path              | def | rep |
        // |--------------------------|-------------------|-----|-----|
        // | Some("Bob")              | name              | 1   | 0   |
        // | None                     | phones.number     | 0   | 0   |
        // | None                     | phones.phone_type | 0   | 0   |
        assert_eq!(parsed.len(), 3);

        // name
        assert_column_striped_value(
            &parsed[0],
            &Value::String(Some("Bob".to_string())),
            1,
            0,
            "Parsed[0] (name)",
        );

        // phones.number
        assert_column_striped_value(
            &parsed[1],
            &Value::String(None),
            0,
            0,
            "Parsed[1] (phones.number - missing)",
        );

        // phones.phone_type
        assert_column_striped_value(
            &parsed[2],
            &Value::String(None),
            0,
            0,
            "Parsed[2] (phones.phone_type - missing)",
        );
    }

    #[test]
    fn test_empty_optional_repeated_group() {
        // message Contact {
        //   optional string name;
        //   repeated group phones {
        //     optional string number;
        //     optional string phone_type;
        //   }
        // }
        let schema = SchemaBuilder::new("Contact")
            .field(optional_string("name"))
            .field(repeated_group(
                "phones",
                vec![optional_string("number"), optional_string("phone_type")],
            ))
            .build();

        // { name: "Charlie", phones: [] }
        let value = ValueBuilder::default()
            .field("name", "Charlie")
            .repeated("phones", Vec::<Value>::new())
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected output:
        // | value                    | path              | def | rep |
        // |--------------------------|-------------------|-----|-----|
        // | Some("Charlie")          | name              | 1   | 0   |
        // | None                     | phones.number     | 1   | 0   |
        // | None                     | phones.phone_type | 1   | 0   |
        assert_eq!(parsed.len(), 3);

        // name
        assert_column_striped_value(
            &parsed[0],
            &Value::String(Some("Charlie".to_string())),
            1,
            0,
            "Parsed[0] (name)",
        );

        // phones.number
        assert_column_striped_value(
            &parsed[1],
            &Value::String(None),
            1,
            0,
            "Parsed[1] (phones.number - empty)",
        );

        // phones.phone_type
        assert_column_striped_value(
            &parsed[2],
            &Value::String(None),
            1,
            0,
            "Parsed[2] (phones.phone_type - empty)",
        );
    }

    #[test]
    fn test_optional_repeated_group_with_single_item() {
        // message Contact {
        //   optional string name;
        //   repeated group phones {
        //     optional string number;
        //     optional string phone_type;
        //   }
        // }
        let schema = SchemaBuilder::new("Contact")
            .field(optional_string("name"))
            .field(repeated_group(
                "phones",
                vec![optional_string("number"), optional_string("phone_type")],
            ))
            .build();

        // { name: "Diana", phones: [{ number: "555-9999", phone_type: "Work" }] }
        let value = ValueBuilder::default()
            .field("name", "Diana")
            .repeated(
                "phones",
                vec![ValueBuilder::default()
                    .field("number", "555-9999")
                    .field("phone_type", "Work")
                    .build()],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected output:
        // | value                    | path              | def | rep |
        // |--------------------------|-------------------|-----|-----|
        // | Some("Diana")            | name              | 1   | 0   |
        // | Some("555-9999")         | phones.number     | 2   | 0   |
        // | Some("Work")             | phones.phone_type | 2   | 0   |
        assert_eq!(parsed.len(), 3);

        // name
        assert_column_striped_value(
            &parsed[0],
            &Value::String(Some("Diana".to_string())),
            1,
            0,
            "Parsed[0] (name)",
        );

        // phones.number
        assert_column_striped_value(
            &parsed[1],
            &Value::String(Some("555-9999".to_string())),
            2,
            0,
            "Parsed[1] (phones.number)",
        );

        // phones.phone_type
        assert_column_striped_value(
            &parsed[2],
            &Value::String(Some("Work".to_string())),
            2,
            0,
            "Parsed[2] (phones.phone_type)",
        );
    }

    #[test]
    fn test_optional_repeated_group_with_missing_name_and_number() {
        // message Contact {
        //   optional string name;
        //   repeated group phones {
        //     optional string number;
        //     optional string phone_type;
        //   }
        // }
        let schema = SchemaBuilder::new("Contact")
            .field(optional_string("name"))
            .field(repeated_group(
                "phones",
                vec![optional_string("number"), optional_string("phone_type")],
            ))
            .build();

        // { phones: [{ phone_type: "Mobile" }] }
        let value = ValueBuilder::default()
            .repeated(
                "phones",
                vec![ValueBuilder::default()
                    .field("phone_type", "Mobile")
                    .build()],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected output:
        // | value                    | path              | def | rep |
        // |--------------------------|-------------------|-----|-----|
        // | Some("Mobile")           | phones.phone_type | 2   | 0   |
        // | None                     | phones.number     | 1   | 0   |
        // | None                     | name              | 0   | 0   |
        assert_eq!(parsed.len(), 3);

        // phones.phone_type
        assert_column_striped_value(
            &parsed[0],
            &Value::String(Some("Mobile".to_string())),
            2,
            0,
            "Parsed[0] (phones.phone_type)",
        );

        // phones.number
        assert_column_striped_value(
            &parsed[1],
            &Value::String(None),
            1,
            0,
            "Parsed[1] (phones.number - missing)",
        );

        // name
        assert_column_striped_value(
            &parsed[2],
            &Value::String(None),
            0,
            0,
            "Parsed[2] (name - missing)",
        );
    }

    #[test]
    fn test_optional_repeated_group_with_multiple_items() {
        // message Contact {
        //   optional string name;
        //   repeated group phones {
        //     optional string number;
        //     optional string phone_type;
        //   }
        // }
        let schema = SchemaBuilder::new("Contact")
            .field(optional_string("name"))
            .field(repeated_group(
                "phones",
                vec![optional_string("number"), optional_string("phone_type")],
            ))
            .build();

        // { name: "Alice", phones: [{number: "555-1111"}, {phone_type: "Home"}, {number: "555-3333", phone_type: "Work"}] }
        let value = ValueBuilder::default()
            .field("name", "Alice")
            .repeated(
                "phones",
                vec![
                    ValueBuilder::default().field("number", "555-1111").build(),
                    ValueBuilder::default().field("phone_type", "Home").build(),
                    ValueBuilder::default()
                        .field("number", "555-3333")
                        .field("phone_type", "Work")
                        .build(),
                ],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // Expected output:
        // | value                    | path              | def | rep |
        // |--------------------------|-------------------|-----|-----|
        // | Some("Alice")            | name              | 1   | 0   |
        // | Some("555-1111")         | phones.number     | 2   | 0   |
        // | None                     | phones.phone_type | 1   | 0   |
        // | Some("Home")             | phones.phone_type | 2   | 1   |
        // | None                     | phones.number     | 1   | 1   |
        // | Some("555-3333")         | phones.number     | 2   | 1   |
        // | Some("Work")             | phones.phone_type | 2   | 1   |
        assert_eq!(parsed.len(), 7);

        // name
        assert_column_striped_value(
            &parsed[0],
            &Value::String(Some("Alice".to_string())),
            1,
            0,
            "Parsed[0] (name)",
        );

        // phones[0].number
        assert_column_striped_value(
            &parsed[1],
            &Value::String(Some("555-1111".to_string())),
            2,
            0,
            "Parsed[1] (phones[0].number)",
        );

        // phones[0].phone_type (missing)
        assert_column_striped_value(
            &parsed[2],
            &Value::String(None),
            1,
            0,
            "Parsed[2] (phones[0].phone_type - missing)",
        );

        // phones[1].phone_type
        assert_column_striped_value(
            &parsed[3],
            &Value::String(Some("Home".to_string())),
            2,
            1,
            "Parsed[3] (phones[1].phone_type)",
        );

        // phones[1].number (missing)
        assert_column_striped_value(
            &parsed[4],
            &Value::String(None),
            1,
            1,
            "Parsed[4] (phones[1].number - missing)",
        );

        // phones[2].number
        assert_column_striped_value(
            &parsed[5],
            &Value::String(Some("555-3333".to_string())),
            2,
            1,
            "Parsed[5] (phones[2].number)",
        );

        // phones[2].phone_type
        assert_column_striped_value(
            &parsed[6],
            &Value::String(Some("Work".to_string())),
            2,
            1,
            "Parsed[6] (phones[2].phone_type)",
        );
    }
}
