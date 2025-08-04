//! Defines the building blocks for defining schemas: [`Field`] and [`DataType`]

use std::fmt::{self, Formatter, Write};

/// Represents the primitive, nested and repeated types from the Dremel data model
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DataType {
    /// Boolean type (true/false)
    Boolean,
    /// Integer type (physical representation maybe i64 type)
    Integer,
    /// String type (UTF-8)
    String,
    /// Repeated type represented by a list of elements. The inner data type of
    /// all list elements are the same.
    List(Box<DataType>),
    /// A nested structure (group/record) containing named fields.
    Struct(Vec<Field>),
}

impl DataType {
    /// Checks if data type is a [`DataType::List`]
    pub fn is_list(&self) -> bool {
        matches!(self, DataType::List(_))
    }

    /// Returns a string label representing the variant of this [`DataType`].
    pub fn type_label(&self) -> String {
        let label = match self {
            DataType::Boolean => "Boolean",
            DataType::Integer => "Integer",
            DataType::String => "String",
            DataType::List(_) => "List", // does not include nested type
            DataType::Struct(_) => "Struct", // does not include fields
        };

        label.into()
    }
}

/// Represents a named schema element, its data type and if the field is
/// optional.
///
/// For a repeated field which is represented by [`DataType::List`], the
/// nullable is implicitly `true`. This matches the semantics where a
/// repeated field can be missing or empty.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Field {
    name: String,
    data_type: DataType,
    /// Indicates if this field is explicitly marked as optional. This
    /// flag is independent of whether a field is repeated. Please use
    /// `is_optional()` to check the effective optionality of nullable
    /// or repeated fields.
    nullable: bool,
}

impl Field {
    /// Creates a field definition.
    ///
    /// # Parameters
    /// * `name` - Name of the field.
    /// * `data_type` - The [`DataType`] of the field.
    /// * `nullable` - `true` if the field is explicitly optional. This is
    ///   independent of repeated fields which implicitly optional regardless
    ///   of the internal state of this field.
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        // Stores the user provided nullable flag directly. The caller is
        // expected to use `is_optional()` which also handles repeated
        // fields.
        Field {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    /// Returns the name of the field.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the [`DataType`] of the field.
    ///
    /// This is an immutable read-only reference so the caller will not be able
    /// to modify the internal state of a field. The primary reason though is
    /// efficiency. When describing complex nested data types this prevents any
    /// unpredictable memory allocations for the caller.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Checks if a field is either explicitly optional or repeated.
    ///
    /// Returns `true` if the field was marked `nullable` during creation
    /// OR if it is a [`DataType::List`] (repeated). Required fields are
    /// neither nullable nor repeated.
    pub fn is_optional(&self) -> bool {
        self.is_repeated() || self.nullable
    }

    /// Checks if a field is repeated: [`DataType::List`]
    pub fn is_repeated(&self) -> bool {
        self.data_type.is_list()
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.name,
            if self.nullable {
                "optional"
            } else {
                "required"
            },
            self.data_type,
        )
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Integer => write!(f, "Integer"),
            DataType::String => write!(f, "String"),
            DataType::List(ref inner) => write!(f, "List [ {inner} ]"),
            DataType::Struct(fields) => {
                writeln!(f, "Struct {{")?;
                let mut buf = String::new();
                for field in fields.iter() {
                    writeln!(buf, "  {field},")?;
                }
                writeln!(
                    f,
                    "{}",
                    buf.lines()
                        .map(|line| format!(" {line}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                )?;
                write!(f, "}}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_field() {
        let field = Field::new("name", DataType::String, false);

        assert_eq!(field.name(), "name");
        assert_eq!(field.data_type(), &DataType::String);
        assert!(!field.is_optional());
    }

    #[test]
    fn test_nested_record() {
        let name = Field::new("name", DataType::String, false);
        let age = Field::new("age", DataType::Integer, false);
        let verified = Field::new("verified", DataType::Boolean, false);
        let emails = Field::new("emails", DataType::List(Box::new(DataType::String)), false);

        let person = Field::new(
            "person",
            DataType::Struct(vec![name, age, verified, emails]),
            false,
        );

        match person.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(
                    fields.len(),
                    4,
                    "Top-level struct 'person' should contain exactly 4 fields, found {}",
                    fields.len()
                );

                assert_eq!(fields[3].name(), "emails");
                match fields[3].data_type() {
                    DataType::List(items) => {
                        assert_eq!(**items, DataType::String);
                    }
                    _ => panic!(
                        "Expected 'emails' to be a `List(String)` type, found {:?}",
                        fields[3].data_type()
                    ),
                }
            }
            _ => panic!(
                "Expected 'person' to be a `Struct` type, found {:?}",
                person.data_type()
            ),
        }
    }

    #[test]
    fn test_deeply_nested_record() {
        let d = Field::new("d", DataType::String, false);
        let c = Field::new("c", DataType::Integer, false);
        let b = Field::new("b", DataType::Struct(vec![c, d]), false);
        let a = Field::new("a", DataType::Struct(vec![b]), false);

        match a.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(
                    fields.len(),
                    1,
                    "Top-level struct '{}' should contain exactly 1 field, found {}",
                    a.name(),
                    fields.len()
                );
                assert_eq!(
                    fields[0].name(),
                    "b",
                    "Expected field name 'b' in struct 'a' but found '{}'",
                    fields[0].name()
                );

                match fields[0].data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(
                            fields.len(),
                            2,
                            "Nested struct 'b' should have only 2 fields, found {}",
                            fields.len()
                        );

                        assert_eq!(fields[0].name(), "c");
                        assert_eq!(fields[0].data_type(), &DataType::Integer);
                        assert_eq!(fields[1].name(), "d");
                        assert_eq!(fields[1].data_type(), &DataType::String);
                    }
                    _ => panic!(
                        "Field 'b' expected to be a `Struct`, found {:?}",
                        fields[0].data_type()
                    ),
                }
            }
            _ => panic!(
                "Field 'a' expected to be a `Struct`, found {:?}.",
                a.data_type()
            ),
        }
    }
}
