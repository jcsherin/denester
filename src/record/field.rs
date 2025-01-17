#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    String,
    List(Box<Field>),
    Struct(Vec<Field>),
}

#[derive(Debug, PartialEq)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
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
        assert_eq!(field.is_nullable(), false);
    }

    #[test]
    fn test_nested_record() {
        let name = Field::new("name", DataType::String, false);
        let age = Field::new("age", DataType::Integer, false);
        let verified = Field::new("verified", DataType::Boolean, false);
        let email = Field::new("email", DataType::String, false);
        let contacts = Field::new("contacts", DataType::List(Box::new(email)), true);

        let person = Field::new(
            "person",
            DataType::Struct(vec![name, age, verified, contacts]),
            false,
        );

        match person.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 4);

                assert_eq!(fields[3].name(), "contacts");
                match fields[3].data_type() {
                    DataType::List(email) => {
                        assert_eq!(email.name(), "email");
                        assert_eq!(email.data_type(), &DataType::String);
                        assert_eq!(email.is_nullable(), false);
                    }
                    _ => panic!(
                        "Expected {:?} to be a `List` data type",
                        fields[3].data_type()
                    ),
                }
            }
            _ => panic!(
                "Expected {:?} to be a `Struct` data type.",
                person.data_type()
            ),
        }
    }
}
