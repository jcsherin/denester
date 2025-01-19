use crate::record::{DataType, Field};

#[derive(Debug)]
pub struct Schema {
    name: String,
    fields: Vec<Field>,
}

impl Schema {
    pub fn new(name: impl Into<String>, fields: Vec<Field>) -> Self {
        Self {
            name: name.into(),
            fields,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

pub struct SchemaBuilder {
    name: String,
    fields: Vec<Field>,
}

impl SchemaBuilder {
    pub fn new(name: impl Into<String>, fields: Vec<Field>) -> Self {
        Self {
            name: name.into(),
            fields,
        }
    }

    pub fn field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    pub fn build(self) -> Schema {
        Schema::new(self.name, self.fields)
    }
}

pub fn bool(name: &str) -> Field {
    Field::new(name, DataType::Boolean, false)
}

pub fn integer(name: &str) -> Field {
    Field::new(name, DataType::Integer, false)
}

pub fn string(name: &str) -> Field {
    Field::new(name, DataType::String, false)
}

pub fn optional_bool(name: &str) -> Field {
    Field::new(name, DataType::Boolean, true)
}

pub fn optional_integer(name: &str) -> Field {
    Field::new(name, DataType::Integer, true)
}

pub fn optional_string(name: &str) -> Field {
    Field::new(name, DataType::String, true)
}

pub fn repeated_bool(name: &str) -> Field {
    Field::new(name, DataType::List(Box::new(DataType::Boolean)), true)
}

pub fn repeated_integer(name: &str) -> Field {
    Field::new(name, DataType::List(Box::new(DataType::Integer)), true)
}

pub fn repeated_string(name: &str) -> Field {
    Field::new(name, DataType::List(Box::new(DataType::String)), true)
}

pub fn required_group(name: &str, fields: Vec<Field>) -> Field {
    Field::new(name, DataType::Struct(fields), false)
}

pub fn optional_group(name: &str, fields: Vec<Field>) -> Field {
    Field::new(name, DataType::Struct(fields), true)
}

pub fn repeated_group(name: &str, fields: Vec<Field>) -> Field {
    Field::new(
        name,
        DataType::List(Box::new(DataType::Struct(fields))),
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_schema() {
        let empty = Schema::new("empty", vec![]);

        assert_eq!(empty.name(), "empty");
        assert_eq!(empty.fields().len(), 0);
    }

    #[test]
    fn test_flat_schema() {
        let fields = vec![
            Field::new("userid", DataType::Integer, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("email", DataType::String, false),
        ];

        let schema = Schema::new("account", fields);

        assert_eq!(schema.name(), "account");
        assert_eq!(schema.fields().len(), 3);
    }

    /// Test creation of document schema provided as an example in the
    /// paper: Dremel: Interactive Analysis of Web-Scale Datasets
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
    #[test]
    fn test_nested_schema() {
        let doc = SchemaBuilder::new("Document", vec![])
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
            .build();

        assert_eq!(doc.name(), "Document");
        assert_eq!(doc.fields().len(), 3);

        //   required int64 DocId;
        assert_eq!(doc.fields()[0].name(), "DocId");
        assert_eq!(doc.fields()[0].data_type(), &DataType::Integer);
        assert_eq!(doc.fields()[0].is_nullable(), false);

        //   optional group Links {
        //     repeated int64 Backward;
        //     repeated int64 Forward;
        //   }
        assert_eq!(doc.fields()[1].name(), "Links");
        assert_eq!(doc.fields()[1].is_nullable(), true);
        if let DataType::Struct(fields) = doc.fields()[1].data_type() {
            assert_eq!(fields.len(), 2);

            assert_eq!(fields[0].name(), "Backward");
            assert_eq!(
                fields[0].data_type(),
                &DataType::List(Box::new(DataType::Integer))
            );
            assert_eq!(fields[0].is_nullable(), true);

            assert_eq!(fields[1].name(), "Forward");
            assert_eq!(
                fields[1].data_type(),
                &DataType::List(Box::new(DataType::Integer))
            );
            assert_eq!(fields[1].is_nullable(), true);
        } else {
            panic!(
                "Links should be an optional group, found: {:?}",
                doc.fields()[1].data_type()
            );
        }

        //   repeated group Name {
        //     repeated group Language {
        //       required string Code;
        //       optional string Country;
        //     }
        //     optional string Url;
        //   }
        if let DataType::List(name_group) = doc.fields()[2].data_type() {
            if let DataType::Struct(fields) = name_group.as_ref() {
                assert_eq!(fields.len(), 2);

                assert_eq!(fields[0].name(), "Language");
                assert_eq!(fields[0].is_nullable(), true);
                //     repeated group Language {
                //       required string Code;
                //       optional string Country;
                //     }
                if let DataType::List(language_group) = fields[0].data_type() {
                    if let DataType::Struct(fields) = language_group.as_ref() {
                        assert_eq!(fields.len(), 2);

                        assert_eq!(fields[0].name(), "Code");
                        assert_eq!(fields[0].is_nullable(), false);
                        assert_eq!(fields[0].data_type(), &DataType::String);

                        assert_eq!(fields[1].name(), "Country");
                        assert_eq!(fields[1].is_nullable(), true);
                        assert_eq!(fields[1].data_type(), &DataType::String);
                    } else {
                        panic!(
                            "Inner Language should be a struct found: {:?}",
                            language_group
                        )
                    }
                } else {
                    panic!(
                        "Outer Language should be a repeated group, found: {:?}",
                        fields[0].data_type()
                    )
                }

                assert_eq!(fields[1].name(), "Url");
                assert_eq!(fields[1].data_type(), &DataType::String);
                assert_eq!(fields[1].is_nullable(), true);
            } else {
                panic!("Inner Name should be a struct, found: {:?}", name_group)
            }
        } else {
            panic!(
                "Outer Name should be a repeated group, found: {:?}",
                doc.fields()[2].data_type()
            );
        }
    }
}
