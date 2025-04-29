use crate::common::{DefinitionLevel, RepetitionLevel};
use crate::field::{DataType, Field};
use crate::path_vector::{PathVector, PathVectorSlice};
use crate::schema::Schema;
use std::fmt::{Display, Formatter};
use std::slice::Iter;

#[derive(Debug)]
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

#[derive(Debug)]
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

impl Iterator for FieldPathIterator<'_> {
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
    definition_level: DefinitionLevel,
    repetition_level: RepetitionLevel,
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

    pub fn definition_level(&self) -> DefinitionLevel {
        self.definition_level
    }

    pub fn repetition_level(&self) -> RepetitionLevel {
        self.repetition_level
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
                .unwrap_or_else(|| {
                    panic!(
                        "Field with name: {:?} not found in path: {:?}",
                        name,
                        field_path.path()
                    )
                });

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

#[derive(Debug)]
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

impl Iterator for PathMetadataIterator<'_> {
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
    use crate::field_path::{FieldPath, FieldPathIterator, PathMetadata};
    use crate::schema::{bool, integer, required_group, string, SchemaBuilder};

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
}
