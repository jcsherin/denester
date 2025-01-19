#[derive(Debug, PartialEq)]
pub enum Value {
    Boolean(bool),
    Integer(i64),
    String(String),
    // List(Vec<Value>),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let null = Value::Null;
        assert_eq!(Value::Null, null);
    }

    #[test]
    fn test_primitives() {
        let b = Value::from(true);
        assert_eq!(Value::Boolean(true), b);

        let n = Value::from(1234);
        assert_eq!(Value::Integer(1234), n);

        let s = Value::from("hello world");
        assert_eq!(Value::String("hello world".to_string()), s);
    }
}
