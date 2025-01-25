use std::fmt;
use std::fmt::Formatter;

/// None is used to represent NULL values corresponding to the type
/// `Value::Boolean(None)` is a `Boolean` NULL value.
#[derive(Debug, PartialEq)]
pub enum Value {
    Boolean(Option<bool>),
    Integer(Option<i64>),
    String(Option<String>),
    List(Vec<Value>),
    Struct(Vec<(String, Value)>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(value) => write!(
                f,
                "{}",
                value.map_or(String::from("<null>"), |b| b.to_string())
            ),
            Value::Integer(value) => write!(
                f,
                "{}",
                value.map_or(String::from("<null>"), |n| n.to_string())
            ),
            Value::String(value) => write!(f, "{}", value.as_ref().map_or("<null>", |s| s)),
            Value::List(values) if values.is_empty() => write!(f, "[]"),
            Value::List(values) => {
                write!(f, "[")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) if fields.is_empty() => write!(f, "{{}}"),
            Value::Struct(fields) => {
                writeln!(f, " {{ ")?;
                for (k, v) in fields {
                    writeln!(f, "  {}: {},", k, v)?;
                }
                write!(f, " }} ")
            }
        }
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

pub struct ValueBuilder {
    fields: Vec<(String, Value)>,
}

impl ValueBuilder {
    pub fn new() -> Self {
        Self { fields: vec![] }
    }

    pub fn field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

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

    pub fn optional_boolean(self, key: impl Into<String>, value: Option<bool>) -> Self {
        self.field(key, <Option<bool> as Into<Value>>::into(value))
    }

    pub fn optional_integer(self, key: impl Into<String>, value: Option<i64>) -> Self {
        self.field(key, <Option<i64> as Into<Value>>::into(value))
    }

    pub fn optional_string(self, key: impl Into<String>, value: Option<&str>) -> Self {
        self.field(key, <Option<&str> as Into<Value>>::into(value))
    }

    pub fn build(self) -> Value {
        Value::Struct(self.fields)
    }
}

pub struct DepthFirstValueIterator<'a> {
    stack: Vec<&'a Value>,
}

impl Value {
    pub fn iter_depth_first(&self) -> DepthFirstValueIterator {
        DepthFirstValueIterator { stack: vec![self] }
    }
}

impl<'a> Iterator for DepthFirstValueIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.stack.pop() {
            match value {
                Value::Boolean(_) | Value::Integer(_) | Value::String(_) => return Some(value),
                Value::List(values) => {
                    self.stack.extend(values.iter().rev());
                }
                Value::Struct(fields) => {
                    self.stack.extend(fields.iter().rev().map(|(_, v)| v));
                    return Some(value);
                }
            }
        }
        None
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
        let actual = ValueBuilder::new().build();
        let expected = Value::Struct(vec![]);

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_builder_required_fields() {
        let actual = ValueBuilder::new()
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
        let actual = ValueBuilder::new()
            .optional_string("name", None::<&str>)
            .optional_integer("id", None::<i64>)
            .optional_boolean("enrolled", None::<bool>)
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
        let actual = ValueBuilder::new()
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
        let actual = ValueBuilder::new()
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
        let actual = ValueBuilder::new()
            .field(
                "a",
                ValueBuilder::new()
                    .repeated(
                        "b",
                        vec![
                            ValueBuilder::new()
                                .field("c", false)
                                .field("d", 1011)
                                .build(),
                            ValueBuilder::new()
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
        assert_eq!(items, vec![&Value::Boolean(Some(true))]);

        let value = Value::from(100);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert_eq!(items, vec![&Value::Integer(Some(100))]);

        let value = Value::from("hello");
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert_eq!(items, vec![&Value::String(Some(String::from("hello")))]);
    }

    #[test]
    fn test_depth_first_empty_list() {
        let value = Value::List(vec![]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 0);
    }
    #[test]
    fn test_depth_first_list() {
        let value = Value::List(vec![
            Value::Boolean(Some(true)),
            Value::Boolean(Some(false)),
            Value::Boolean(None),
        ]);
        let items = value.iter_depth_first().collect::<Vec<&Value>>();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0], &Value::Boolean(Some(true)));
        assert_eq!(items[1], &Value::Boolean(Some(false)));
        assert_eq!(items[2], &Value::Boolean(None));
    }

    #[test]
    fn test_depth_first_empty_struct() {
        let value = Value::Struct(vec![]);
        let items = value.iter_depth_first().collect::<Vec<_>>();

        assert_eq!(items.len(), 1);
        assert!(matches!(items[0], Value::Struct(_)));
    }

    #[test]
    fn test_depth_first_struct() {
        let value = Value::Struct(vec![
            ("name".to_string(), Value::String(None)),
            ("age".to_string(), Value::Integer(None)),
            ("active".to_string(), Value::Boolean(None)),
        ]);
        let items = value.iter_depth_first().collect::<Vec<&Value>>();

        assert_eq!(items.len(), 4);
        assert!(matches!(items[0], Value::Struct(_)));
        assert_eq!(items[1], &Value::String(None));
        assert_eq!(items[2], &Value::Integer(None));
        assert_eq!(items[3], &Value::Boolean(None));
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
        let items = value.iter_depth_first().collect::<Vec<&Value>>();

        assert_eq!(items.len(), 10);
        assert!(matches!(items[0], Value::Struct(_)));
        assert_eq!(items[1], &Value::Integer(Some(10)));
        assert_eq!(items[2], &Value::String(Some(String::from("x"))));
        assert_eq!(items[3], &Value::String(Some(String::from("y"))));
        assert!(matches!(items[4], Value::Struct(_)));
        assert_eq!(items[5], &Value::Integer(Some(20)));
        assert_eq!(items[6], &Value::Integer(Some(30)));
        assert_eq!(items[7], &Value::Integer(Some(40)));
        assert_eq!(items[8], &Value::Integer(Some(50)));
        assert_eq!(items[9], &Value::String(None));
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
        let items = value.iter_depth_first().collect::<Vec<&Value>>();

        assert_eq!(items.len(), 8);
        assert!(matches!(items[0], Value::Struct(_)));
        assert!(matches!(items[3], Value::Struct(_)));
        assert!(matches!(items[5], Value::Struct(_)));
        assert!(matches!(items[7], Value::Struct(_)));

        assert_eq!(items[1], &Value::Integer(Some(10)));
        assert_eq!(items[2], &Value::Integer(Some(20)));
        assert_eq!(items[4], &Value::Integer(Some(30)));
        assert_eq!(items[6], &Value::Integer(Some(40)));
    }
}
