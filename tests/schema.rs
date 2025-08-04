use denester::field::{DataType, Field};
use denester::schema::test_utils::create_doc;

/// Integration tests using the example schema from the [Dremel paper]
///
/// [Dremel paper]: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf
mod schema_validation {
    use super::*;

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
        assert!(
            !doc_id.is_optional(),
            "DocId should be required (not optional)"
        );
    }

    #[test]
    fn test_links_group() {
        let doc = create_doc();
        let links = &doc.fields()[1];

        assert_eq!(links.name(), "Links", "Second field should be Links");
        assert!(links.is_optional(), "Links group should be optional");
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
                assert!(backward.is_optional(), "Backward should be optional");

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
                assert!(forward.is_optional(), "Forward should be optional");
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
                    panic!("Name list should contain Struct elements, found: {name_type}",)
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
                    assert!(url.is_optional(), "Url should be optional");
                }
            },
        }
    }

    // Helper to test Language group structure
    fn test_language_group(fields: &[Field]) {
        let language = &fields[0];

        assert_eq!(
            language.name(),
            "Language",
            "First field in Name should be Language"
        );
        assert!(language.is_optional(), "Language should be optional");

        match language.data_type() {
            DataType::Boolean | DataType::Integer | DataType::String | DataType::Struct(_) => {
                panic!(
                    "Language should be a List type, found: {}",
                    language.data_type()
                )
            }
            DataType::List(language_type) => match language_type.as_ref() {
                DataType::Boolean | DataType::Integer | DataType::String | DataType::List(_) => {
                    panic!("Language list should contain Struct elements, found: {language_type}",)
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
                    assert!(
                        !code.is_optional(),
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
                    assert!(country.is_optional(), "Country should be optional");
                    assert_eq!(
                        country.data_type(),
                        &DataType::String,
                        "Country should be a String type"
                    );
                }
            },
        }
    }
}
