use crate::field::{DataType, Field};
use crate::field_path::FieldLevel;
use crate::path_vector::PathVector;
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

    pub fn is_empty(&self) -> bool {
        self.fields().is_empty()
    }
}

pub struct DepthFirstSchemaIterator<'a> {
    stack: Vec<FieldLevel<'a>>,
}

impl<'a> Iterator for DepthFirstSchemaIterator<'a> {
    type Item = (&'a Field, PathVector);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(field_level) = self.stack.last_mut() {
            if let Some(field) = field_level.next() {
                let mut path = field_level.path().clone();
                path.push(field.name().to_string());

                match field.data_type() {
                    DataType::List(item_type) => {
                        if let DataType::Struct(children) = item_type.as_ref() {
                            self.stack
                                .push(FieldLevel::new(children.iter(), path.clone()));
                        }
                    }
                    DataType::Struct(children) => {
                        self.stack
                            .push(FieldLevel::new(children.iter(), path.clone()));
                    }
                    DataType::Boolean | DataType::Integer | DataType::String => {}
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
}
