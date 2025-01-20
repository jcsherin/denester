/// None is used to represent NULL values corresponding to the type
/// `Value::Boolean(None)` is a `Boolean` NULL value.
#[derive(Debug, PartialEq)]
pub enum Value {
    Boolean(Option<bool>),
    Integer(Option<i64>),
    String(Option<String>),
    List(Vec<Value>),
    // Struct(Vec<String, Value>),
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
}
