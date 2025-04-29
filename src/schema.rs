//! Defines the representation of nested data schemas ([`Schema`]) and provides
//! functions for building and iterating over them.
//!
//! Use the [`SchemaBuilder`] for schema construction, aided by helper functions
//! like [`integer()`], [`string()`], [`optional_group()`], [`repeated_group()`]
//! which simplify creating [`Field`] definitions.

use crate::field::{DataType, Field};
use std::fmt::{self, Formatter, Write};

/// Represents the schema definition for a nested data structure.
///
/// A schema consists of a name and list of top-level fields. Fields can
/// be primitive (integer, string, bool) fields, nested fields (struct)
/// and repeated fields (list).
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    name: String,
    fields: Vec<Field>,
}

impl Schema {
    /// Creates a new schema with a given name and top-level fields.
    ///
    /// # Parameters
    /// * `name` - The schema name.
    /// * `fields` - The top-level [`Field`] definitions.
    pub fn new(name: impl Into<String>, fields: Vec<Field>) -> Self {
        Self {
            name: name.into(),
            fields,
        }
    }

    /// Returns the name of the schema
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a slice of the top-level fields in the schema.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Checks if the schema defines any fields.
    pub fn is_empty(&self) -> bool {
        self.fields().is_empty()
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

/// Ergonomic builder pattern API for creating nested schema definitions.
#[derive(Debug, Clone)]
pub struct SchemaBuilder {
    name: String,
    fields: Vec<Field>,
}

impl SchemaBuilder {
    /// Creates a new `SchemaBuilder` with a given name and no fields.
    ///
    /// Fields can be added using [`SchemaBuilder::field`].
    ///
    /// # Parameters
    /// * `name` - The name for the schema being built.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
        }
    }

    /// Adds a field to the schema being built.
    ///
    /// # Parameters
    /// * `field` - The [`Field`] definition to add.
    ///
    /// # Example
    /// ```rust
    /// # use denester::schema::{integer, string};
    /// # use denester::SchemaBuilder;
    ///
    /// let builder = SchemaBuilder::new("ExampleSchema")
    ///     .field(string("name"))
    ///     .field(integer("age"));
    /// ```
    pub fn field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    /// Consumes the builder and returns the constructed [`Schema`]
    pub fn build(self) -> Schema {
        Schema::new(self.name, self.fields)
    }
}

// --- Helper functions for Field creation ---

/// Creates a required boolean field.
pub fn bool(name: &str) -> Field {
    Field::new(name, DataType::Boolean, false)
}

/// Creates a required integer field.
pub fn integer(name: &str) -> Field {
    Field::new(name, DataType::Integer, false)
}

/// Creates a required string field.
pub fn string(name: &str) -> Field {
    Field::new(name, DataType::String, false)
}

/// Creates an optional boolean field.
pub fn optional_bool(name: &str) -> Field {
    Field::new(name, DataType::Boolean, true)
}

/// Creates an optional integer field.
pub fn optional_integer(name: &str) -> Field {
    Field::new(name, DataType::Integer, true)
}

/// Creates an optional string field.
pub fn optional_string(name: &str) -> Field {
    Field::new(name, DataType::String, true)
}

/// Creates a repeated boolean field which is represented as a list of booleans.
/// Repeated fields are implicitly optional meaning they can be either missing
/// or empty.
pub fn repeated_bool(name: &str) -> Field {
    Field::new(name, DataType::List(Box::new(DataType::Boolean)), true)
}

/// Creates a repeated integer field which is represented as a list of integers.
/// Repeated fields are implicitly optional meaning they can be either missing
/// or empty.
pub fn repeated_integer(name: &str) -> Field {
    Field::new(name, DataType::List(Box::new(DataType::Integer)), true)
}

/// Creates a repeated string field which is represented as a list of strings.
/// Repeated fields are implicitly optional meaning they can be either missing
/// or empty.
pub fn repeated_string(name: &str) -> Field {
    Field::new(name, DataType::List(Box::new(DataType::String)), true)
}

/// Creates a required group field (struct).
///
/// # Parameters
/// * `name` - The name of the group field.
/// * `fields` - The collection of [`Field`] definitions in this group.
pub fn required_group(name: &str, fields: Vec<Field>) -> Field {
    Field::new(name, DataType::Struct(fields), false)
}

/// Creates an optional group field (struct).
///
/// # Parameters
/// * `name` - The name of the group field.
/// * `fields` - The collection of [`Field`] definitions in this group.
pub fn optional_group(name: &str, fields: Vec<Field>) -> Field {
    Field::new(name, DataType::Struct(fields), true)
}

/// Creates a repeated group field (list of structs).
///
/// # Parameters
/// * `name` - The name of the repeated group field.
/// * `fields` - The collection of [`Field`] definitions defining the structure
///     of the structs within the list.
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
