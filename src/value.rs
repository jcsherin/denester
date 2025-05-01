//! Defines the representation of nested data structure values.

use crate::field::{DataType, Field};
use crate::path_vector::PathVector;
use crate::ValuePath;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

/// Represents the concrete instance of nested data structure.
///
/// There is a one-one correspondence with [`DataType`] enum. This is useful
/// to recursively type-check a concrete value against a schema to see if the
/// value conforms to the schema.
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    /// Boolean value (true/false) or null value
    Boolean(Option<bool>),
    /// Signed integer value or null value
    Integer(Option<i64>),
    /// String (UTF-8) value or null value
    String(Option<String>),
    /// Repeated value represented as a list of elements. All list elements
    /// are the same type. If there are zero elements it means this value
    /// is considered empty.
    List(Vec<Value>),
    /// A nested structure (group/record) containing name, value pairs. If
    /// there are zero name, value pairs, then this value is considered empty.
    Struct(Vec<(String, Value)>),
}

impl Value {
    fn fmt_with_indent(&self, f: &mut Formatter<'_>, indent: usize) -> fmt::Result {
        match self {
            Value::Boolean(value) => write!(f, "Value::Boolean({:?})", value,),
            Value::Integer(value) => write!(f, "Value::Integer({:?})", value),
            Value::String(value) => write!(f, "Value::String({:?})", value),
            Value::List(values) if values.is_empty() => write!(f, "Value::List(items: [])"),
            Value::List(values) => {
                write!(f, "Value::List(items: [")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, "])")
            }
            Value::Struct(fields) if fields.is_empty() => write!(f, "Value::Struct({{}})"),
            Value::Struct(fields) => {
                writeln!(f, "{{")?;
                for (k, v) in fields {
                    write!(f, "{:indent$}", "", indent = indent + 2)?;
                    write!(f, "{}: ", k)?;
                    v.fmt_with_indent(f, indent + 2)?;
                    writeln!(f, ",")?;
                }
                write!(f, "{:indent$}}}", "", indent = indent)
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(Some(value))
    }
}

impl From<Option<bool>> for Value {
    fn from(value: Option<bool>) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(Some(value))
    }
}

impl From<Option<i64>> for Value {
    fn from(value: Option<i64>) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(Some(value.to_string()))
    }
}

impl From<Option<&str>> for Value {
    fn from(value: Option<&str>) -> Self {
        Self::String(value.map(String::from))
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(values: Vec<T>) -> Self {
        Self::List(values.into_iter().map(Into::into).collect())
    }
}

impl From<Vec<(String, Value)>> for Value {
    fn from(fields: Vec<(String, Value)>) -> Self {
        Self::Struct(fields)
    }
}

/// Ergonomic builder pattern API for creating a concrete nested value.
#[derive(Debug, Default, Clone)]
pub struct ValueBuilder {
    fields: Vec<(String, Value)>,
}

impl ValueBuilder {
    /// Add a name, value pair to the value being built.
    pub fn field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    /// Add a name, repeated value to the value being built.
    ///
    /// A repeated value is represented as a [`Value::List`] of [`Value`]
    /// elements. But nested lists are not permitted.
    pub fn repeated(
        mut self,
        key: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<Value>>,
    ) -> Self {
        self.fields.push((
            key.into(),
            Value::List(values.into_iter().map(Into::into).collect()),
        ));
        self
    }

    /// Add a boolean type value or null value
    pub fn boolean(self, key: impl Into<String>, value: Option<bool>) -> Self {
        self.field(key, <Option<bool> as Into<Value>>::into(value))
    }

    /// Add a integer type value or null value
    pub fn integer(self, key: impl Into<String>, value: Option<i64>) -> Self {
        self.field(key, <Option<i64> as Into<Value>>::into(value))
    }

    /// Add a string type value or null value
    pub fn string(self, key: impl Into<String>, value: Option<&str>) -> Self {
        self.field(key, <Option<&str> as Into<Value>>::into(value))
    }

    /// Consumes the builder and returns the constructed [`Value`]
    pub fn build(self) -> Value {
        Value::Struct(self.fields)
    }
}

impl Value {
    /// Returns either a null or an empty value based on the [`DataType`].
    ///
    /// # Parameters
    /// * `data_type` - The [`DataType`] of the value.
    pub fn create_null_or_empty(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => Value::Boolean(None),
            DataType::Integer => Value::Integer(None),
            DataType::String => Value::String(None),
            DataType::List(_) => Value::List(vec![]),
            DataType::Struct(_) => Value::Struct(vec![]),
        }
    }

    /// Returns a depth-first iterator for a [`Value`].
    pub fn iter_depth_first(&self) -> DepthFirstValueIterator {
        DepthFirstValueIterator {
            stack: vec![(self, PathVector::default())],
        }
    }

    /// Performs shallow type checking for a given value.
    ///
    /// For repeated values `Value::List(_)` the individual items in the list are not type checked.
    /// Similarly, for records `Value::Struct(_)` the field names should match. The values are not
    /// type checked.
    ///
    /// Returns a type-checking error if value does not match field definition.
    ///
    /// The type-checking performed is shallow and does not attempt to validate the entire nested
    /// data structure. This is not a limitation but guarantees stable performance. A recursive
    /// type-checking of the entire value can be implemented lazily by composing
    /// [`Value::iter_depth_first`] and calling this method after visiting each value node.
    ///
    /// This means,
    /// - For [`Value::Struct`] type-checking is delegated to [`Value::type_check_struct_shallow`].
    /// - For [`Value::List`] type-checking validates that it is [`DataType::List`] but then the
    ///   elements in the list are skipped.
    /// - For other primitive/scalar types [`Value`] is compared against the field [`DataType`]
    ///   definition.
    pub(crate) fn type_check_shallow(
        &self,
        field: &Field,
        path: &PathVector,
    ) -> Result<(), TypeCheckError> {
        match (self, field.data_type()) {
            (Value::Boolean(_), DataType::Boolean)
            | (Value::Integer(_), DataType::Integer)
            | (Value::String(_), DataType::String) => {
                if !field.is_optional() && self.is_null() {
                    Err(TypeCheckError::RequiredFieldIsNull {
                        path: path.to_vec(),
                        field: field.clone(),
                    })
                } else {
                    Ok(())
                }
            }
            (Value::List(_), DataType::List(_)) => Ok(()),
            (Value::Struct(props), DataType::Struct(fields)) => {
                Self::type_check_struct_shallow(path, props, fields)
            }
            _ => Err(TypeCheckError::DataTypeMismatch {
                value: self.clone(),
                field: field.clone(),
            }),
        }
    }

    /// Checks if a primitive value is null.
    ///
    /// Always returns false for group(struct) or repeated(list) values.
    pub fn is_null(&self) -> bool {
        match self {
            Value::Boolean(b) => b.is_none(),
            Value::Integer(i) => i.is_none(),
            Value::String(s) => s.is_none(),
            Value::List(_) | Value::Struct(_) => false,
        }
    }

    /// Checks if all name, value pairs in a struct matches the given fields definitions.
    ///
    /// Returns a [`TypeCheckError`] if,
    /// - There exists duplicate names.
    /// - If any required field is missing.
    /// - If a name is not found in any of the field definitions.
    ///
    /// The ordering of name, value pairs does not have to match the order of the field definitions.
    ///
    /// # Parameters
    /// - `path` - The path from root of the value used in [`TypeCheckError`]
    /// - `props` - The name, value pairs extracted from [`Value::Struct`]
    /// - `fields` - The field definitions extracted from [`DataType::Struct`]`
    pub(crate) fn type_check_struct_shallow(
        path: &PathVector,
        props: &[(String, Value)],
        fields: &[Field],
    ) -> Result<(), TypeCheckError> {
        // If there are more named value pairs than defined fields in the struct, it cannot be a
        // valid match. Fewer values are allowed as some fields maybe repeated/optional.
        if props.len() > fields.len() {
            return Err(TypeCheckError::StructSchemaMismatch {
                props: props.into(),
                fields: fields.into(),
            });
        }

        // Duplicate names are not allowed
        let mut seen_names = HashSet::new();
        for (name, _) in props {
            if !seen_names.insert(name) {
                return Err(TypeCheckError::StructDuplicateProperty {
                    dup: name.clone(),
                    props: props.into(),
                    fields: fields.into(),
                });
            }
        }

        let mut field_names = HashSet::new();
        let mut required_fields = HashSet::new();
        for field in fields {
            field_names.insert(field.name());
            if !field.is_optional() {
                required_fields.insert(field.name());
            }
        }

        let mut present_fields = HashSet::new();
        for (name, _) in props {
            if !field_names.contains(name.as_str()) {
                return Err(TypeCheckError::StructUnknownProperty {
                    unknown: name.clone(),
                    props: props.into(),
                    fields: fields.into(),
                });
            }
            present_fields.insert(name.as_str());
        }

        // All required fields are present
        if !required_fields.is_subset(&present_fields) {
            let missing = required_fields
                .difference(&present_fields)
                .copied()
                .map(|name| name.to_string())
                .collect::<Vec<_>>();
            Err(TypeCheckError::RequiredFieldsAreMissing {
                missing,
                path: path.to_vec(),
            })
        } else {
            Ok(())
        }
    }
}

/// TODO: add docs
#[derive(Debug)]
pub enum TypeCheckError {
    DataTypeMismatch {
        value: Value,
        field: Field,
    },
    RequiredFieldIsNull {
        path: Vec<String>,
        field: Field,
    },
    RequiredFieldsAreMissing {
        missing: Vec<String>,
        path: Vec<String>,
    },
    StructSchemaMismatch {
        props: Vec<(String, Value)>,
        fields: Vec<Field>,
    },
    StructDuplicateProperty {
        dup: String,
        props: Vec<(String, Value)>,
        fields: Vec<Field>,
    },
    StructUnknownProperty {
        unknown: String,
        props: Vec<(String, Value)>,
        fields: Vec<Field>,
    },
}

/// A depth-first iterator for [`Value`].
///
/// The iterator works like a cursor which visits every node in the [`Value`]
/// tree in a depth-first order. It yields a tuple whose first element is a
/// reference to the [`Value`] node, and the second element is a data structure
/// representing the path from the root to the current node.
#[derive(Debug)]
pub struct DepthFirstValueIterator<'a> {
    stack: Vec<(&'a Value, PathVector)>,
}

impl<'a> Iterator for DepthFirstValueIterator<'a> {
    type Item = (&'a Value, ValuePath);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((value, current_path)) = self.stack.pop() {
            match value {
                Value::Boolean(_) | Value::Integer(_) | Value::String(_) => {
                    Some((value, current_path.to_vec()))
                }
                Value::List(values) => {
                    for item in values.iter().rev() {
                        self.stack.push((item, current_path.clone()))
                    }
                    Some((value, current_path.to_vec()))
                }
                Value::Struct(fields) => {
                    for (key, value) in fields.iter().rev() {
                        let mut new_path = current_path.clone();
                        new_path.push(key.to_string());
                        self.stack.push((value, new_path));
                    }
                    Some((value, current_path.to_vec()))
                }
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direct_primitive_conversion() {
        assert_eq!(Value::Boolean(Some(true)), Value::from(true));
        assert_eq!(Value::Boolean(Some(false)), Value::from(false));

        assert_eq!(Value::Integer(Some(42)), Value::from(42));

        assert_eq!(
            Value::String(Some(String::from("hello world"))),
            Value::from("hello world")
        );
    }

    #[test]
    fn test_optional_primitive_conversion() {
        assert_eq!(Value::Boolean(Some(true)), Value::from(Some(true)));
        assert_eq!(Value::Boolean(Some(false)), Value::from(Some(false)));
        assert_eq!(Value::Boolean(None), Value::from(None::<bool>));

        assert_eq!(Value::Integer(Some(42)), Value::from(Some(42)));
        assert_eq!(Value::Integer(None), Value::from(None::<i64>));

        assert_eq!(
            Value::String(Some(String::from("hello world"))),
            Value::from(Some("hello world"))
        );
        assert_eq!(Value::String(None), Value::from(None::<&str>));
    }

    #[test]
    fn test_repeated_values_without_null() {
        assert_eq!(
            Value::List(vec![
                Value::Boolean(Some(true)),
                Value::Boolean(Some(false)),
            ]),
            Value::from(vec![true, false])
        );

        assert_eq!(
            Value::List(vec![
                Value::Boolean(Some(true)),
                Value::Boolean(Some(false)),
            ]),
            Value::from(vec![Some(true), Some(false)])
        );

        assert_eq!(
            Value::List(vec![Value::Integer(Some(100)), Value::Integer(Some(200)),]),
            Value::from(vec![100, 200])
        );

        assert_eq!(
            Value::List(vec![Value::Integer(Some(100)), Value::Integer(Some(200)),]),
            Value::from(vec![Some(100), Some(200)])
        );

        assert_eq!(
            Value::List(vec![
                Value::String(Some(String::from("hello"))),
                Value::String(Some(String::from("world"))),
            ]),
            Value::from(vec!["hello", "world"])
        );

        assert_eq!(
            Value::List(vec![
                Value::String(Some(String::from("hello"))),
                Value::String(Some(String::from("world"))),
            ]),
            Value::from(vec![Some("hello"), Some("world")])
        );
    }

    #[test]
    fn test_empty_list_types() {
        assert_eq!(Value::List(vec![]), Value::from(Vec::<bool>::new()));
        assert_eq!(Value::List(vec![]), Value::from(Vec::<i64>::new()));
        assert_eq!(Value::List(vec![]), Value::from(Vec::<&str>::new()));
    }

    #[test]
    fn test_repeated_values_including_null() {
        assert_eq!(
            Value::List(vec![
                Value::Boolean(None),
                Value::Boolean(Some(true)),
                Value::Boolean(None),
                Value::Boolean(Some(false)),
                Value::Boolean(None),
            ]),
            Value::from(vec![None, Some(true), None, Some(false), None])
        );

        assert_eq!(
            Value::List(vec![Value::Integer(Some(42)), Value::Integer(None)]),
            Value::from(vec![Some(42), None])
        );

        assert_eq!(
            Value::List(vec![
                Value::String(Some(String::from("hello"))),
                Value::String(None)
            ]),
            Value::from(vec![Some("hello"), None])
        );
    }

    #[test]
    fn test_empty_struct() {
        assert_eq!(
            Value::Struct(vec![]),
            Value::from(Vec::<(String, Value)>::new())
        );
    }

    #[test]
    fn test_basic_struct() {
        let actual = Value::Struct(vec![
            (String::from("name"), Value::from("Patricia")),
            (String::from("id"), Value::from(1001)),
            (String::from("enrolled"), Value::from(false)),
        ]);

        let expected = Value::Struct(vec![
            (String::from("name"), Value::String(Some("Patricia".into()))),
            (String::from("id"), Value::Integer(Some(1001))),
            (String::from("enrolled"), Value::Boolean(Some(false))),
        ]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_struct_with_null_values() {
        let actual = Value::Struct(vec![
            (String::from("name"), Value::from(None::<&str>)),
            (String::from("id"), Value::from(None::<i64>)),
            (String::from("enrolled"), Value::from(None::<bool>)),
        ]);

        let expected = Value::Struct(vec![
            (String::from("name"), Value::String(None)),
            (String::from("id"), Value::Integer(None)),
            (String::from("enrolled"), Value::Boolean(None)),
        ]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_nested_struct() {
        let c = Value::from(vec![(String::from("c"), Value::from(true))]);
        let b = Value::from(vec![(String::from("b"), c)]);
        let a = Value::from(vec![(String::from("a"), b)]);

        let expected = Value::Struct(vec![(
            String::from("a"),
            Value::Struct(vec![(
                String::from("b"),
                Value::Struct(vec![(String::from("c"), Value::Boolean(Some(true)))]),
            )]),
        )]);

        assert_eq!(a, expected)
    }

    #[test]
    fn test_repeated_struct() {
        let item1 = Value::from(vec![(
            String::from("b"),
            Value::from(vec![
                (String::from("c"), Value::from(true)),
                (String::from("d"), Value::from(101)),
            ]),
        )]);
        let item2 = Value::from(vec![(
            String::from("b"),
            Value::from(vec![
                (String::from("c"), Value::from(false)),
                (String::from("d"), Value::from(301)),
            ]),
        )]);
        let actual = Value::from(vec![(String::from("a"), Value::from(vec![item1, item2]))]);

        let expected_item1 = Value::Struct(vec![(
            String::from("b"),
            Value::Struct(vec![
                (String::from("c"), Value::from(true)),
                (String::from("d"), Value::from(101)),
            ]),
        )]);
        let expected_item2 = Value::Struct(vec![(
            String::from("b"),
            Value::Struct(vec![
                (String::from("c"), Value::from(false)),
                (String::from("d"), Value::from(301)),
            ]),
        )]);
        let expected = Value::Struct(vec![(
            String::from("a"),
            Value::List(vec![expected_item1, expected_item2]),
        )]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_empty_builder() {
        let actual = ValueBuilder::default().build();
        let expected = Value::Struct(vec![]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_builder_required_fields() {
        let actual = ValueBuilder::default()
            .field("name", "Patricia")
            .field("id", 1001)
            .field("enrolled", true)
            .field("groups", vec![1, 2, 3])
            .build();

        let expected = Value::Struct(vec![
            (
                "name".to_string(),
                Value::String(Some("Patricia".to_string())),
            ),
            ("id".to_string(), Value::Integer(Some(1001))),
            ("enrolled".to_string(), Value::Boolean(Some(true))),
            (
                "groups".to_string(),
                Value::List(vec![
                    Value::Integer(Some(1)),
                    Value::Integer(Some(2)),
                    Value::Integer(Some(3)),
                ]),
            ),
        ]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_builder_optional_fields() {
        let actual = ValueBuilder::default()
            .string("name", None::<&str>)
            .integer("id", None::<i64>)
            .boolean("enrolled", None::<bool>)
            .repeated("groups", Vec::<i64>::new())
            .build();

        let expected = Value::Struct(vec![
            ("name".to_string(), Value::String(None)),
            ("id".to_string(), Value::Integer(None)),
            ("enrolled".to_string(), Value::Boolean(None)),
            ("groups".to_string(), Value::List(vec![])),
        ]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_builder_repeated_fields() {
        let actual = ValueBuilder::default()
            .repeated("empty", Vec::<Value>::new())
            .repeated("non-empty", vec![1, 2])
            .build();

        let expected = Value::Struct(vec![
            ("empty".to_string(), Value::List(vec![])),
            (
                "non-empty".to_string(),
                Value::List(vec![Value::Integer(Some(1)), Value::Integer(Some(2))]),
            ),
        ]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_builder_repeated_with_nulls() {
        let actual = ValueBuilder::default()
            .repeated("xs", vec![Some(1), None, Some(2), None])
            .build();

        let expected = Value::Struct(vec![(
            "xs".to_string(),
            Value::List(vec![
                Value::Integer(Some(1)),
                Value::Integer(None),
                Value::Integer(Some(2)),
                Value::Integer(None),
            ]),
        )]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_builder_nested_fields() {
        let actual = ValueBuilder::default()
            .field(
                "a",
                ValueBuilder::default()
                    .repeated(
                        "b",
                        vec![
                            ValueBuilder::default()
                                .field("c", false)
                                .field("d", 1011)
                                .build(),
                            ValueBuilder::default()
                                .field("c", true)
                                .field("d", 1010)
                                .build(),
                        ],
                    )
                    .build(),
            )
            .build();

        let expected = Value::Struct(vec![(
            "a".to_string(),
            Value::Struct(vec![(
                "b".to_string(),
                Value::List(vec![
                    Value::Struct(vec![
                        ("c".to_string(), Value::Boolean(Some(false))),
                        ("d".to_string(), Value::Integer(Some(1011))),
                    ]),
                    Value::Struct(vec![
                        ("c".to_string(), Value::Boolean(Some(true))),
                        ("d".to_string(), Value::Integer(Some(1010))),
                    ]),
                ]),
            )]),
        )]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_depth_first_primitives() {
        let value = Value::from(true);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, &Value::Boolean(Some(true)));
        assert!(items[0].1.is_empty());

        let value = Value::from(100);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, &Value::Integer(Some(100)));
        assert!(items[0].1.is_empty());

        let value = Value::from("hello");
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, &Value::String(Some("hello".to_string())));
        assert!(items[0].1.is_empty());
    }

    #[test]
    fn test_depth_first_empty_list() {
        let value = Value::List(vec![]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert!(matches!(items[0].0, &Value::List(_)), "empty list node");
    }
    #[test]
    fn test_depth_first_list() {
        let value = Value::List(vec![
            Value::Boolean(Some(true)),
            Value::Boolean(Some(false)),
            Value::Boolean(None),
        ]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 4);
        assert!(matches!(items[0].0, &Value::List(_)));
        assert_eq!(items[1].0, &Value::Boolean(Some(true)));
        assert_eq!(items[2].0, &Value::Boolean(Some(false)));
        assert_eq!(items[3].0, &Value::Boolean(None));

        assert!(items[0].1.is_empty());
        assert!(items[1].1.is_empty());
        assert!(items[2].1.is_empty());
        assert!(items[3].1.is_empty());
    }

    #[test]
    fn test_depth_first_empty_struct() {
        let value = Value::Struct(vec![]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert!(matches!(items[0].0, Value::Struct(_)));
    }

    #[test]
    fn test_depth_first_struct() {
        let value = Value::Struct(vec![
            ("name".to_string(), Value::String(None)),
            ("age".to_string(), Value::Integer(None)),
            ("active".to_string(), Value::Boolean(None)),
        ]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 4);

        assert!(matches!(items[0].0, Value::Struct(_)));
        assert_eq!(items[1].0, &Value::String(None));
        assert_eq!(items[2].0, &Value::Integer(None));
        assert_eq!(items[3].0, &Value::Boolean(None));

        assert!(items[0].1.is_empty());
        assert_eq!(items[1].1, ["name"]);
        assert_eq!(items[2].1, ["age"]);
        assert_eq!(items[3].1, ["active"]);
    }

    #[test]
    fn test_depth_first_nested_struct() {
        let value = Value::Struct(vec![
            ("a".to_string(), Value::Integer(Some(10))),
            (
                "b".to_string(),
                Value::List(vec![
                    Value::String(Some(String::from("x"))),
                    Value::String(Some(String::from("y"))),
                ]),
            ),
            (
                "c".to_string(),
                Value::Struct(vec![
                    (
                        "p".to_string(),
                        Value::List(vec![Value::Integer(Some(20)), Value::Integer(Some(30))]),
                    ),
                    (
                        "q".to_string(),
                        Value::List(vec![Value::Integer(Some(40)), Value::Integer(Some(50))]),
                    ),
                ]),
            ),
            ("d".to_string(), Value::String(None)),
        ]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 13);

        assert!(matches!(items[0].0, Value::Struct(_)));
        assert!(items[0].1.is_empty());

        assert_eq!(items[1].0, &Value::Integer(Some(10)));
        assert_eq!(items[1].1, ["a"]);

        assert!(matches!(items[2].0, Value::List(_)));
        assert_eq!(items[3].0, &Value::String(Some(String::from("x"))));
        assert_eq!(items[4].0, &Value::String(Some(String::from("y"))));
        assert_eq!(items[3].1, ["b"]);
        assert_eq!(items[4].1, ["b"]);

        assert!(matches!(items[5].0, Value::Struct(_)));
        assert_eq!(items[5].1, ["c"]);

        assert!(matches!(items[6].0, Value::List(_)));
        assert_eq!(items[7].0, &Value::Integer(Some(20)));
        assert_eq!(items[8].0, &Value::Integer(Some(30)));
        assert_eq!(items[7].1, ["c", "p"]);
        assert_eq!(items[8].1, ["c", "p"]);

        assert!(matches!(items[9].0, Value::List(_)));
        assert_eq!(items[10].0, &Value::Integer(Some(40)));
        assert_eq!(items[11].0, &Value::Integer(Some(50)));
        assert_eq!(items[10].1, ["c", "q"]);
        assert_eq!(items[11].1, ["c", "q"]);

        assert_eq!(items[12].0, &Value::String(None));
        assert_eq!(items[12].1, ["d"]);
    }

    #[test]
    fn test_depth_first_repeated_structs() {
        let value = Value::List(vec![
            Value::Struct(vec![
                ("x".to_string(), Value::Integer(Some(10))),
                ("y".to_string(), Value::Integer(Some(20))),
            ]),
            Value::Struct(vec![("x".to_string(), Value::Integer(Some(30)))]),
            Value::Struct(vec![("y".to_string(), Value::Integer(Some(40)))]),
            Value::Struct(vec![]),
        ]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 9);
        assert!(matches!(items[0].0, Value::List(_)));
        assert!(matches!(items[1].0, Value::Struct(_)));
        assert!(matches!(items[4].0, Value::Struct(_)));
        assert!(matches!(items[6].0, Value::Struct(_)));
        assert!(matches!(items[8].0, Value::Struct(_)));

        assert!(items[0].1.is_empty());
        assert!(items[1].1.is_empty());
        assert!(items[4].1.is_empty());
        assert!(items[6].1.is_empty());
        assert!(items[8].1.is_empty());

        assert_eq!(items[2].0, &Value::Integer(Some(10)));
        assert_eq!(items[3].0, &Value::Integer(Some(20)));
        assert_eq!(items[5].0, &Value::Integer(Some(30)));
        assert_eq!(items[7].0, &Value::Integer(Some(40)));

        assert_eq!(items[2].1, ["x"]);
        assert_eq!(items[3].1, ["y"]);
        assert_eq!(items[5].1, ["x"]);
        assert_eq!(items[7].1, ["y"]);
    }
}
