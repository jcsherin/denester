use crate::record::{DataType, Field, FieldLevel, PathVector};
use std::fmt;
use std::fmt::Formatter;
use std::fmt::Write;

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

    pub fn iter_depth_first(&self) -> DepthFirstSchemaIterator {
        DepthFirstSchemaIterator {
            stack: vec![FieldLevel::new(self.fields.iter(), vec![])],
        }
    }
}

pub struct DepthFirstSchemaIterator<'a> {
    stack: Vec<FieldLevel<'a>>,
}

impl<'a> Iterator for DepthFirstSchemaIterator<'a> {
    type Item = (&'a Field, PathVector);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(field_level) = self.stack.last_mut() {
            if let Some(field) = field_level.iter.next() {
                let mut path = field_level.path.clone();
                path.push(field.name().to_string());

                match field.data_type() {
                    DataType::List(item_type) => match item_type.as_ref() {
                        DataType::Struct(children) => {
                            self.stack
                                .push(FieldLevel::new(children.iter(), path.clone()));
                        }
                        _ => {}
                    },
                    DataType::Struct(children) => {
                        self.stack
                            .push(FieldLevel::new(children.iter(), path.clone()));
                    }
                    _ => {}
                }

                return Some((field, path));
            } else {
                self.stack.pop();
            }
        }

        None
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut buf = String::new();

        writeln!(&mut buf, "{} {{", self.name)?;
        for field in &self.fields {
            writeln!(&mut buf, "{}", field)?;
        }
        writeln!(&mut buf, "}}")?;

        write!(f, "{}", buf)
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

    #[test]
    fn test_required_fields() {
        let b = bool("bool");
        let i = integer("integer");
        let s = string("string");

        assert!(
            !b.is_optional(),
            "Expected Boolean field to be required, found: {:?}",
            b
        );
        assert!(
            !i.is_optional(),
            "Expected Integer field to be required, found: {:?}",
            i
        );
        assert!(
            !s.is_optional(),
            "Expected String field to be required, found: {:?}",
            s
        );
    }

    #[test]
    fn test_optional_fields() {
        let ob = optional_bool("bool");
        let oi = optional_integer("integer");
        let os = optional_string("string");

        assert!(
            ob.is_optional(),
            "Expected Boolean field to be optional, found {:?}",
            ob
        );
        assert!(
            oi.is_optional(),
            "Expected Integer field to be optional, found {:?}",
            oi
        );
        assert!(
            os.is_optional(),
            "Expected String field to be optional, found {:?}",
            os
        );
    }

    #[test]
    fn test_repeated_fields() {
        let rb = repeated_bool("bool");
        let ri = repeated_integer("integer");
        let rs = repeated_string("string");

        assert_eq!(
            rb.data_type(),
            &DataType::List(Box::new(DataType::Boolean)),
            "Expected List(Boolean) found, {:?}",
            rb
        );
        assert!(
            rb.is_optional(),
            "Expected Boolean field to be optional, found: {:?}",
            rb
        );

        assert_eq!(
            ri.data_type(),
            &DataType::List(Box::new(DataType::Integer)),
            "Expected List(Integer) found, {:?}",
            ri
        );
        assert!(
            ri.is_optional(),
            "Expected Integer field to be optional, found {:?}",
            ri
        );

        assert_eq!(
            rs.data_type(),
            &DataType::List(Box::new(DataType::String)),
            "Expected List(String) found, {:?}",
            rs
        );
        assert!(
            rs.is_optional(),
            "Expected String field to be optional, found {:?}",
            rs
        );
    }

    fn parse_group(group: &Field) {
        match group.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 0);
            }
            other => {
                panic!("Expected Group to be Struct, found: {:?}", other);
            }
        }
    }
    #[test]
    fn test_group() {
        let rg = required_group("required_group", vec![]);
        let og = optional_group("optional_group", vec![]);

        assert!(
            !rg.is_optional(),
            "Expected Boolean field to be required, found {:?}",
            rg
        );
        assert!(
            og.is_optional(),
            "Expected Boolean field to be required, found {:?}",
            og
        );

        parse_group(&rg);
        parse_group(&og);
    }

    #[test]
    fn test_repeated_group() {
        let repeated_group = repeated_group("repeated_group", vec![]);

        assert!(
            repeated_group.is_optional(),
            "Expected repeated Group to be optional, found {:?}",
            repeated_group
        );

        if let DataType::List(groups) = repeated_group.data_type() {
            if let DataType::Struct(fields) = groups.as_ref() {
                assert_eq!(fields.len(), 0);
            }
        } else {
            panic!(
                "Expected List(Struct<...fields>) type, found {:?}",
                repeated_group.data_type()
            );
        }
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
        assert_eq!(doc.fields()[0].is_optional(), false);

        //   optional group Links {
        //     repeated int64 Backward;
        //     repeated int64 Forward;
        //   }
        assert_eq!(doc.fields()[1].name(), "Links");
        assert_eq!(doc.fields()[1].is_optional(), true);
        if let DataType::Struct(fields) = doc.fields()[1].data_type() {
            assert_eq!(fields.len(), 2);

            assert_eq!(fields[0].name(), "Backward");
            assert_eq!(
                fields[0].data_type(),
                &DataType::List(Box::new(DataType::Integer))
            );
            assert_eq!(fields[0].is_optional(), true);

            assert_eq!(fields[1].name(), "Forward");
            assert_eq!(
                fields[1].data_type(),
                &DataType::List(Box::new(DataType::Integer))
            );
            assert_eq!(fields[1].is_optional(), true);
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
                assert_eq!(fields[0].is_optional(), true);
                //     repeated group Language {
                //       required string Code;
                //       optional string Country;
                //     }
                if let DataType::List(language_group) = fields[0].data_type() {
                    if let DataType::Struct(fields) = language_group.as_ref() {
                        assert_eq!(fields.len(), 2);

                        assert_eq!(fields[0].name(), "Code");
                        assert_eq!(fields[0].is_optional(), false);
                        assert_eq!(fields[0].data_type(), &DataType::String);

                        assert_eq!(fields[1].name(), "Country");
                        assert_eq!(fields[1].is_optional(), true);
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
                assert_eq!(fields[1].is_optional(), true);
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
