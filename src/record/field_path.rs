use crate::record::{DataType, Field};

#[derive(Debug)]
pub struct FieldPath<'a> {
    field: &'a Field,
    path: Vec<String>,
}

impl<'a> FieldPath<'a> {
    pub fn new(field: &'a Field, path: Vec<String>) -> Self {
        Self { field, path }
    }

    pub fn field(&self) -> &'a Field {
        &self.field
    }

    pub fn path(&self) -> &[String] {
        &self.path
    }
}

pub struct FieldPathIterator<'a> {
    stack: Vec<(std::slice::Iter<'a, Field>, Vec<String>)>,
}

impl<'a> Iterator for FieldPathIterator<'a> {
    type Item = FieldPath<'a>;

    /**
    message Order {
        required OrderId integer,
        repeated Items group {
            required ItemId integer,
            optional Quantity integer,
            repeated Tags string,
        },
        required Customer group {
            required Name string,
            repeated Addresses group {
                required MobileNo integer,
                optional Email string,
            }
        },
    }

    Path enumeration:
    - OrderId
    - Items.ItemId
    - Items.Quantity
    - Items.Tag
    - Customer.Name
    - Customer.Addresses.MobileNo
    - Customer.Addresses.Email

    Stack Traversal:
    1. Push TopLevel Iterator; path = []
    2. Push Items Iterator; path = ["items"]
    3. Pop Items Iterator
    4. Push Customer Iterator; path = ["customer"]
    5. Push Addresses Iterator; path = ["customer", "addresses"]
    6. Pop Addresses Iterator
    7. Pop Customer Iterator
    8. Pop TopLevel Iterator
    **/
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((field_iter, current_path)) = self.stack.last_mut() {
            if let Some(field) = field_iter.next() {
                let mut path = current_path.clone();
                path.push(field.name().to_string());

                match field.data_type() {
                    DataType::Struct(inner_fields) => {
                        self.stack.push((inner_fields.iter(), path));
                        continue;
                    }
                    DataType::List(inner_type) => match inner_type.as_ref() {
                        DataType::Struct(inner_fields) => {
                            self.stack.push((inner_fields.iter(), path));
                            continue;
                        }
                        _ => return Some(FieldPath::new(field, path)),
                    },
                    DataType::Boolean | DataType::Integer | DataType::String => {
                        return Some(FieldPath::new(field, path));
                    }
                }
            }
            self.stack.pop();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::record::field_path::{FieldPath, FieldPathIterator};
    use crate::record::schema::{bool, integer, required_group, string};
    use crate::record::{DataType, Field, SchemaBuilder};

    #[test]
    fn test_field_path() {
        let email = Field::new("email", DataType::String, true);
        let path = vec![
            String::from("customer"),
            String::from("address"),
            String::from("email"),
        ];

        let actual = FieldPath::new(&email, path);

        assert_eq!(actual.field(), &email);
        assert_eq!(actual.path()[0], String::from("customer"));
        assert_eq!(actual.path()[1], String::from("address"));
        assert_eq!(actual.path()[2], String::from("email"));
    }

    #[test]
    fn test_empty_schema() {
        let schema = SchemaBuilder::new("empty", vec![]).build();

        let iter = FieldPathIterator {
            stack: vec![(schema.fields().iter(), vec![])],
        };
        let paths = iter.collect::<Vec<FieldPath>>();

        assert_eq!(paths.len(), 0);
    }

    #[test]
    fn test_flat_record_paths() {
        let schema = SchemaBuilder::new("test", vec![])
            .field(integer("id"))
            .field(string("name"))
            .field(bool("active"))
            .build();

        let iter = FieldPathIterator {
            stack: vec![(schema.fields().iter(), vec![])],
        };
        let paths = iter.collect::<Vec<FieldPath>>();
        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].path(), vec!["id"]);
        assert_eq!(paths[0].field.data_type(), &DataType::Integer);

        assert_eq!(paths[1].path(), vec!["name"]);
        assert_eq!(paths[1].field.data_type(), &DataType::String);

        assert_eq!(paths[2].path(), vec!["active"]);
        assert_eq!(paths[2].field.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_nested_record_paths() {
        let schema = SchemaBuilder::new("test", vec![])
            .field(required_group(
                "user",
                vec![integer("id"), string("name"), bool("active")],
            ))
            .build();

        let iter = FieldPathIterator {
            stack: vec![(schema.fields().iter(), vec![])],
        };
        let paths = iter.collect::<Vec<FieldPath>>();

        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].path(), vec!["user", "id"]);
        assert_eq!(paths[0].field.data_type(), &DataType::Integer);

        assert_eq!(paths[1].path(), vec!["user", "name"]);
        assert_eq!(paths[1].field.data_type(), &DataType::String);

        assert_eq!(paths[2].path(), vec!["user", "active"]);
        assert_eq!(paths[2].field.data_type(), &DataType::Boolean);
    }
}
