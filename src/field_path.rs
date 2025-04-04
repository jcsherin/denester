use crate::field::{DataType, Field};
use crate::path_vector::{PathVector, PathVectorSlice};
use crate::schema::Schema;
use std::fmt::{Display, Formatter};
use std::slice::Iter;

pub(crate) struct FieldLevel<'a> {
    iter: Iter<'a, Field>,
    path: PathVector,
}

impl<'a> FieldLevel<'a> {
    pub fn new(iter: Iter<'a, Field>, path: Vec<String>) -> Self {
        Self { iter, path }
    }

    pub fn next(&mut self) -> Option<&'a Field> {
        self.iter.next()
    }

    pub fn path(&self) -> &PathVector {
        &self.path
    }
}
#[derive(Debug, Clone)]
pub struct FieldPath {
    field: Field,
    path: PathVector,
}

impl FieldPath {
    pub fn new(field: Field, path: PathVector) -> Self {
        Self { field, path }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn path(&self) -> PathVectorSlice {
        &self.path
    }
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "path: {} field:{}", self.path.join("."), self.field)
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
    type Item = FieldPath;

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
                        return Some(FieldPath {
                            field: field.clone(),
                            path,
                        })
                    }
                    DataType::List(datatype) => match datatype.as_ref() {
                        DataType::Struct(fields) => {
                            self.levels.push(FieldLevel::new(fields.iter(), path))
                        }
                        _ => {
                            return Some(FieldPath {
                                field: field.clone(),
                                path,
                            })
                        }
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

#[derive(Debug, Clone)]
pub struct PathMetadata {
    field_path: FieldPath,
    definition_level: u8,
    repetition_level: u8,
}

impl PathMetadata {
    pub fn new(schema: &Schema, field_path: FieldPath) -> Self {
        let (definition_level, repetition_level) = Self::compute_levels(schema, &field_path);

        Self {
            field_path,
            definition_level,
            repetition_level,
        }
    }

    pub fn path(&self) -> PathVectorSlice {
        self.field_path.path()
    }

    pub fn len(&self) -> usize {
        self.path().len()
    }

    pub fn field(&self) -> &Field {
        self.field_path.field()
    }

    pub fn max_repetition_level(&self) -> u8 {
        self.repetition_level
    }

    pub fn max_definition_level(&self) -> u8 {
        self.definition_level
    }

    fn compute_levels(schema: &Schema, field_path: &FieldPath) -> (u8, u8) {
        let mut definition_level = 0;
        let mut repetition_level = 0;

        let mut current_fields = schema.fields();
        for name in field_path.path() {
            let field = current_fields
                .iter()
                .find(|f| f.name() == name)
                .expect(&format!(
                    "Field with name: {:?} not found in path: {:?}",
                    name,
                    field_path.path()
                ));

            match field.data_type() {
                DataType::Boolean | DataType::Integer | DataType::String | DataType::Struct(_) => {
                    if field.is_optional() {
                        definition_level += 1;
                    }
                }
                DataType::List(_) => {
                    definition_level += 1;
                    repetition_level += 1;
                }
            }

            current_fields = match field.data_type() {
                DataType::List(datatype) => {
                    if let DataType::Struct(fields) = datatype.as_ref() {
                        fields
                    } else {
                        &[]
                    }
                }
                DataType::Struct(fields) => fields,
                _ => &[],
            };
        }

        (definition_level, repetition_level)
    }
}

impl Display for PathMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} max_def: {} max_rep: {}",
            self.field_path, self.definition_level, self.repetition_level
        )
    }
}

pub struct PathMetadataIterator<'a> {
    schema: &'a Schema,
    field_path_iter: FieldPathIterator<'a>,
}

impl<'a> PathMetadataIterator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            field_path_iter: FieldPathIterator::new(schema),
        }
    }
}

impl<'a> Iterator for PathMetadataIterator<'a> {
    type Item = PathMetadata;

    fn next(&mut self) -> Option<Self::Item> {
        self.field_path_iter
            .next()
            .map(|field_path| PathMetadata::new(self.schema, field_path))
    }
}

#[cfg(test)]
mod tests {
    use crate::field::{DataType, Field};
    use crate::field_path::{FieldPath, FieldPathIterator, PathMetadata, PathMetadataIterator};
    use crate::schema::{
        bool, integer, optional_group, optional_integer, optional_string, repeated_group,
        repeated_integer, required_group, string, SchemaBuilder,
    };

    #[test]
    fn test_field_path() {
        let email = Field::new("email", DataType::String, true);
        let path = vec![
            String::from("customer"),
            String::from("address"),
            String::from("email"),
        ];

        let actual = FieldPath::new(email.clone(), path);

        assert_eq!(actual.field(), &email);
        assert_eq!(actual.path()[0], String::from("customer"));
        assert_eq!(actual.path()[1], String::from("address"));
        assert_eq!(actual.path()[2], String::from("email"));
    }

    #[test]
    fn test_empty_field_path_iterator() {
        let schema = SchemaBuilder::new("empty", vec![]).build();
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 0);
    }

    #[test]
    fn test_basic_field_path_iterator() {
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
    fn test_nested_field_path_iterator() {
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
    fn test_dremel_schema_field_path_iterator() {
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

    #[test]
    fn test_basic_field_path_metadata() {
        let schema = SchemaBuilder::new("test", vec![])
            .field(integer("id"))
            .build();
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();

        let id_metadata = PathMetadata::new(&schema, paths[0].clone());

        assert_eq!(*id_metadata.field(), integer("id"));
        assert_eq!(id_metadata.path(), vec!["id"]);
        assert_eq!(id_metadata.definition_level, 0);
        assert_eq!(id_metadata.repetition_level, 0);
    }

    #[test]
    fn test_nested_field_path_metadata() {
        let schema = SchemaBuilder::new("test", vec![])
            .field(required_group("user", vec![string("name")]))
            .build();
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();

        let path_metadata = PathMetadata::new(&schema, paths[0].clone());

        assert_eq!(*path_metadata.field(), string("name"));
        assert_eq!(path_metadata.path(), vec!["user", "name"]);
        assert_eq!(path_metadata.definition_level, 0);
        assert_eq!(path_metadata.repetition_level, 0);
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
    ///
    ///
    /// | Path                  | Definition Level | Repetition Level |
    /// |-----------------------|------------------|------------------|
    /// | DocId                 | 0                | 0                |
    /// | Links.Backward        | 2                | 1                |
    /// | Links.Forward         | 2                | 1                |
    /// | Name.Language.Code    | 2                | 2                |
    /// | Name.Language.Country | 3                | 2                |
    /// | Name.Url              | 2                | 1                |
    /// ```
    #[test]
    fn test_dremel_schema_path_metadata() {
        let schema = SchemaBuilder::new("dremel", vec![])
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

        let actual = PathMetadata::new(&schema, paths[0].clone());
        assert_eq!(actual.field().data_type(), &DataType::Integer);
        assert_eq!(actual.field().name(), "DocId");
        assert_eq!(actual.field().is_optional(), false);
        assert_eq!(actual.path(), vec!["DocId"]);
        assert_eq!(actual.definition_level, 0);
        assert_eq!(actual.repetition_level, 0);

        let actual = PathMetadata::new(&schema, paths[1].clone());
        assert_eq!(
            actual.field().data_type(),
            &DataType::List(Box::new(DataType::Integer))
        );
        assert_eq!(actual.field().name(), "Backward");
        assert_eq!(actual.field().is_optional(), true);
        assert_eq!(actual.path(), vec!["Links", "Backward"]);
        assert_eq!(actual.definition_level, 2);
        assert_eq!(actual.repetition_level, 1);

        let actual = PathMetadata::new(&schema, paths[2].clone());
        assert_eq!(
            actual.field().data_type(),
            &DataType::List(Box::new(DataType::Integer))
        );
        assert_eq!(actual.field().name(), "Forward");
        assert_eq!(actual.path(), vec!["Links", "Forward"]);
        assert_eq!(actual.definition_level, 2);
        assert_eq!(actual.repetition_level, 1);

        let actual = PathMetadata::new(&schema, paths[3].clone());
        assert_eq!(actual.field().data_type(), &DataType::String);
        assert_eq!(actual.field().name(), "Code");
        assert_eq!(actual.field().is_optional(), false);
        assert_eq!(actual.path(), vec!["Name", "Language", "Code"]);
        assert_eq!(actual.definition_level, 2);
        assert_eq!(actual.repetition_level, 2);

        let actual = PathMetadata::new(&schema, paths[4].clone());
        assert_eq!(actual.field().data_type(), &DataType::String);
        assert_eq!(actual.field().name(), "Country");
        assert_eq!(actual.field().is_optional(), true);
        assert_eq!(actual.path(), vec!["Name", "Language", "Country"]);
        assert_eq!(actual.definition_level, 3);
        assert_eq!(actual.repetition_level, 2);

        let actual = PathMetadata::new(&schema, paths[5].clone());
        assert_eq!(actual.field().data_type(), &DataType::String);
        assert_eq!(actual.field().name(), "Url");
        assert_eq!(actual.field().is_optional(), true);
        assert_eq!(actual.path(), vec!["Name", "Url"]);
        assert_eq!(actual.definition_level, 2);
        assert_eq!(actual.repetition_level, 1);
    }

    /// Schema:
    /// ```text
    /// message Order {
    ///   required integer id;
    ///   repeated group items {
    ///     required integer item_id;
    ///     optional integer quantity;
    ///   }
    ///   required group customer {
    ///     required string name;
    ///     optional string email;
    ///   }
    /// }
    ///
    /// | Path              | Definition Level | Repetition Level |
    /// |-------------------|------------------|------------------|
    /// | id                | 0                | 0                |
    /// | items.item_id     | 1                | 1                |
    /// | items.quantity    | 2                | 1                |
    /// | customer.name     | 0                | 0                |
    /// | customer.email    | 1                | 0                |
    /// ```
    #[test]
    fn test_path_metadata_iterator() {
        let schema = SchemaBuilder::new("Order", vec![])
            .field(integer("id"))
            .field(repeated_group(
                "items",
                vec![integer("item_id"), optional_integer("quantity")],
            ))
            .field(required_group(
                "customer",
                vec![string("name"), optional_string("email")],
            ))
            .build();

        let path_metadata = PathMetadataIterator::new(&schema).collect::<Vec<_>>();

        assert_eq!(path_metadata.len(), 5);

        assert_eq!(path_metadata[0].path(), ["id"]);
        assert_eq!(path_metadata[0].definition_level, 0);
        assert_eq!(path_metadata[0].repetition_level, 0);

        assert_eq!(path_metadata[1].path(), ["items", "item_id"]);
        assert_eq!(path_metadata[1].definition_level, 1);
        assert_eq!(path_metadata[1].repetition_level, 1);

        assert_eq!(path_metadata[2].path(), ["items", "quantity"]);
        assert_eq!(path_metadata[2].definition_level, 2);
        assert_eq!(path_metadata[2].repetition_level, 1);

        assert_eq!(path_metadata[3].path(), ["customer", "name"]);
        assert_eq!(path_metadata[3].definition_level, 0);
        assert_eq!(path_metadata[3].repetition_level, 0);

        assert_eq!(path_metadata[4].path(), ["customer", "email"]);
        assert_eq!(path_metadata[4].definition_level, 1);
        assert_eq!(path_metadata[4].repetition_level, 0);
    }
}
