use crate::record::Field;

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

#[cfg(test)]
mod tests {
    use crate::record::schema::Schema;
    use crate::record::{DataType, Field};

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
}
