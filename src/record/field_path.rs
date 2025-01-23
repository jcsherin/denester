use crate::record::{DataType, Field, Schema};

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

struct FieldLevel<'a> {
    iter: std::slice::Iter<'a, Field>,
    path: Vec<String>,
}

impl<'a> FieldLevel<'a> {
    fn new(iter: std::slice::Iter<'a, Field>, path: Vec<String>) -> Self {
        Self { iter, path }
    }
}

pub struct FieldPathIterator<'a> {
    levels: Vec<FieldLevel<'a>>,
}

impl<'a> FieldPathIterator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            levels: vec![FieldLevel::new(schema.fields().iter(), vec![])],
        }
    }
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
        while let Some(field_level) = self.levels.last_mut() {
            if let Some(field) = field_level.iter.next() {
                let mut path = field_level.path.clone();
                path.push(field.name().to_string());

                match field.data_type() {
                    DataType::Boolean | DataType::Integer | DataType::String => {
                        return Some(FieldPath { field, path })
                    }
                    DataType::List(datatype) => match datatype.as_ref() {
                        DataType::Struct(fields) => {
                            self.levels.push(FieldLevel::new(fields.iter(), path))
                        }
                        _ => return Some(FieldPath { field, path }),
                    },
                    DataType::Struct(fields) => {
                        self.levels.push(FieldLevel::new(fields.iter(), path))
                    }
                }
            } else {
                self.levels.pop();
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::record::field_path::{FieldPath, FieldPathIterator};
    use crate::record::schema::{
        bool, integer, optional_group, optional_string, repeated_group, repeated_integer,
        required_group, string,
    };
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
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 0);
    }

    #[test]
    fn test_flat_record_paths() {
        let schema = SchemaBuilder::new("test", vec![])
            .field(integer("id"))
            .field(string("name"))
            .field(bool("active"))
            .build();

        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
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

        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].path(), vec!["user", "id"]);
        assert_eq!(paths[0].field.data_type(), &DataType::Integer);

        assert_eq!(paths[1].path(), vec!["user", "name"]);
        assert_eq!(paths[1].field.data_type(), &DataType::String);

        assert_eq!(paths[2].path(), vec!["user", "active"]);
        assert_eq!(paths[2].field.data_type(), &DataType::Boolean);
    }

    /// Schema example from the paper:
    ///     Dremel: Interactive Analysis of Web-Scale Datasets
    ///
    /// ```text
    /// message Document {
    ///   required int64 DocId;
    ///   optional group Links {
    ///     repeated int64 Backward;
    ///     repeated int64 Forward;
    ///   }
    ///   repeated group Name {
    ///     repeated group Language {
    ///       required string Code;
    ///       optional string Country;
    ///     }
    ///     optional string Url;
    ///   }
    /// }
    ///
    /// Paths in Document,
    ///     1. DocId
    ///     2. Links.Backward
    ///     3. Links.Forward
    ///     4. Name.Language.Code
    ///     5. Name.Language.Country
    ///     6. Name.Url
    /// ```
    #[test]
    fn test_complex_schema() {
        let schema = SchemaBuilder::new("complex", vec![])
            .field(integer("DocId"))
            .field(optional_group(
                "Links",
                vec![repeated_integer("Backward"), repeated_integer("Forward")],
            ))
            .field(repeated_group(
                "Name",
                vec![
                    repeated_group("Language", vec![string("Code"), optional_string("Country")]),
                    optional_string("Url"),
                ],
            ))
            .build();

        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 6);

        assert_eq!(paths[0].path(), vec!["DocId"]);
        assert_eq!(paths[1].path(), vec!["Links", "Backward"]);
        assert_eq!(paths[2].path(), vec!["Links", "Forward"]);
        assert_eq!(paths[3].path(), vec!["Name", "Language", "Code"]);
        assert_eq!(paths[4].path(), vec!["Name", "Language", "Country"]);
        assert_eq!(paths[5].path(), vec!["Name", "Url"]);

        assert_eq!(paths[0].field().name(), "DocId");
        assert_eq!(paths[1].field().name(), "Backward");
        assert_eq!(paths[2].field().name(), "Forward");
        assert_eq!(paths[3].field().name(), "Code");
        assert_eq!(paths[4].field().name(), "Country");
        assert_eq!(paths[5].field().name(), "Url");
    }
}
