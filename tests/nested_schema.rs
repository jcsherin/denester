use denester::field::{DataType, Field};
use denester::schema::{
    integer, optional_group, optional_string, repeated_group, repeated_integer, string, Schema,
    SchemaBuilder,
};

/// Integration tests for modeling nested schema
///
/// These tests verify that we can correctly model and access complex nested documents with
/// optional, required and repeated fields. The schema used for testing can be found in the paper:
/// "Dremel: Interactive Analysis of Web-Scale Datasets".

/// Create schema from the example listed in the Dremel paper
///
/// ```text
/// message Document {
///   required int64 DocId;
///   optional group Links {
///     repeated int64 Backward;
///     repeated int64 Forward;
///   }
///   repeated group Name {
///     repeated group Language {
///       required string Code;
///       optional string Country;
///     }
///     optional string Url;
///   }
/// }
/// ```
fn create_doc() -> Schema {
    SchemaBuilder::new("Document", vec![])
        .field(integer("DocId"))
        .field(optional_group(
            "Links",
            vec![repeated_integer("Backward"), repeated_integer("Forward")],
        ))
        .field(repeated_group(
            "Name",
            vec![
                repeated_group("Language", vec![string("Code"), optional_string("Country")]),
                optional_string("Url"),
            ],
        ))
        .build()
}

#[test]
fn test_doc_root() {
    let doc = create_doc();

    assert_eq!(doc.name(), "Document", "Doc name should match");
    assert_eq!(
        doc.fields().len(),
        3,
        "Doc should have exactly 3 fields at the top-level: DocId, Links & Name"
    );
}

#[test]
fn test_docid_field() {
    let doc = create_doc();
    let doc_id = &doc.fields()[0];

    assert_eq!(doc_id.name(), "DocId", "First field should be DocId");
    assert_eq!(
        doc_id.data_type(),
        &DataType::Integer,
        "DocId should be an Integer type"
    );
    assert_eq!(
        doc_id.is_optional(),
        false,
        "DocId should be required (not optional)"
    );
}

#[test]
fn test_links_group() {
    let doc = create_doc();
    let links = &doc.fields()[1];

    assert_eq!(links.name(), "Links", "Second field should be Links");
    assert_eq!(links.is_optional(), true, "Links group should be optional");
    match links.data_type() {
        DataType::Boolean | DataType::Integer | DataType::String | DataType::List(_) => {
            panic!(
                "Links should be a Struct type, found: {}",
                links.data_type()
            );
        }
        DataType::Struct(fields) => {
            assert_eq!(
                fields.len(),
                2,
                "Links group should contain exactly 2 fields"
            );

            // Test Backward field
            let backward = &fields[0];
            assert_eq!(
                backward.name(),
                "Backward",
                "First field in Links should be Backward"
            );
            assert_eq!(
                backward.data_type(),
                &DataType::List(Box::new(DataType::Integer)),
                "Backward should be a List of Integers"
            );
            assert_eq!(backward.is_optional(), true, "Backward should be optional");

            // Test Forward field
            let forward = &fields[1];
            assert_eq!(
                forward.name(),
                "Forward",
                "Second field in Links should be Forward"
            );
            assert_eq!(
                forward.data_type(),
                &DataType::List(Box::new(DataType::Integer)),
                "Forward should be a List of Integers"
            );
            assert_eq!(forward.is_optional(), true, "Forward should be optional");
        }
    }
}

#[test]
fn test_name_group() {
    let doc = create_doc();
    let name = &doc.fields()[2];

    match name.data_type() {
        DataType::Boolean | DataType::Integer | DataType::String | DataType::Struct(_) => {
            panic!("Name should be a List type, found: {}", name.data_type())
        }
        DataType::List(name_type) => match name_type.as_ref() {
            DataType::Boolean | DataType::Integer | DataType::String | DataType::List(_) => {
                panic!(
                    "Name list should contain Struct elements, found: {}",
                    name_type
                )
            }
            DataType::Struct(fields) => {
                assert_eq!(
                    fields.len(),
                    2,
                    "Name group should contain exactly 2 fields"
                );

                // Test Language field
                test_language_group(fields);

                // Test Url field
                let url = &fields[1];

                assert_eq!(url.name(), "Url", "Second field in Name should be Url");
                assert_eq!(
                    url.data_type(),
                    &DataType::String,
                    "Url should be a String type"
                );
                assert_eq!(url.is_optional(), true, "Url should be optional");
            }
        },
    }
}

fn test_language_group(fields: &[Field]) {
    let language = &fields[0];

    assert_eq!(
        language.name(),
        "Language",
        "First field in Name should be Language"
    );
    assert_eq!(language.is_optional(), true, "Language should be optional");

    match language.data_type() {
        DataType::Boolean | DataType::Integer | DataType::String | DataType::Struct(_) => {
            panic!(
                "Language should be a List type, found: {}",
                language.data_type()
            )
        }
        DataType::List(language_type) => match language_type.as_ref() {
            DataType::Boolean | DataType::Integer | DataType::String | DataType::List(_) => {
                panic!(
                    "Language list should contain Struct elements, found: {}",
                    language_type
                )
            }
            DataType::Struct(fields) => {
                assert_eq!(
                    fields.len(),
                    2,
                    "Language group should contain exactly 2 fields"
                );

                // Test Code field
                let code = &fields[0];
                assert_eq!(
                    code.name(),
                    "Code",
                    "First field in Language should be Code"
                );
                assert_eq!(
                    code.is_optional(),
                    false,
                    "Code should be required (not optional)"
                );
                assert_eq!(
                    code.data_type(),
                    &DataType::String,
                    "Code should be a String type"
                );

                // Test Country field
                let country = &fields[1];
                assert_eq!(
                    country.name(),
                    "Country",
                    "Second field in Language should be Country"
                );
                assert_eq!(country.is_optional(), true, "Country should be optional");
                assert_eq!(
                    country.data_type(),
                    &DataType::String,
                    "Country should be a String type"
                );
            }
        },
    }
}
