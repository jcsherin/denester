#[derive(Debug, PartialEq)]
pub enum Value {
    Boolean(bool),
    Integer(i64),
    String(String),
    List(Vec<Value>),
    // Struct(Vec<String, Value>),
    Null,
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
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
    fn test_primitives() {
        let b = Value::from(true);
        assert_eq!(Value::Boolean(true), b);

        let n = Value::from(1234);
        assert_eq!(Value::Integer(1234), n);

        let s = Value::from("hello world");
        assert_eq!(Value::String("hello world".to_string()), s);
    }

    #[test]
    fn test_repeated_values() {
        let empty = Value::from(Vec::<bool>::new());
        assert_eq!(Value::List(vec![]), empty);

        let nulls = Value::from(vec![Value::Null, Value::Null]);
        assert_eq!(Value::List(vec![Value::Null, Value::Null]), nulls);

        let bs = Value::from(vec![true, false, true]);
        assert_eq!(
            Value::List(vec![
                Value::from(true),
                Value::from(false),
                Value::from(true)
            ]),
            bs
        );

        let is = Value::from(vec![1000, 2000, 3000]);
        assert_eq!(
            Value::List(vec![
                Value::from(1000),
                Value::from(2000),
                Value::from(3000)
            ]),
            is
        );
    }
}
