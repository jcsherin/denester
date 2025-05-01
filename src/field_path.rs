use crate::field::{DataType, Field};
use crate::schema::Schema;
use crate::schema_path::SchemaPath;
use crate::{DefinitionLevel, RepetitionLevel};
use std::fmt::{Display, Formatter};
use std::slice::Iter;

#[derive(Debug)]
struct FieldLevel<'a> {
    iter: Iter<'a, Field>,
    path: SchemaPath,
}

impl<'a> FieldLevel<'a> {
    fn new(iter: Iter<'a, Field>, path: SchemaPath) -> Self {
        Self { iter, path }
    }
}

#[derive(Debug)]
struct FieldPathIterator<'a> {
    levels: Vec<FieldLevel<'a>>,
}

impl<'a> FieldPathIterator<'a> {
    fn new(schema: &'a Schema) -> Self {
        Self {
            levels: vec![FieldLevel::new(
                schema.fields().iter(),
                SchemaPath::default(),
            )],
        }
    }
}

impl Iterator for FieldPathIterator<'_> {
    type Item = (Field, SchemaPath);

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
                        return Some((field.clone(), path))
                    }
                    DataType::List(datatype) => match datatype.as_ref() {
                        DataType::Struct(fields) => {
                            self.levels.push(FieldLevel::new(fields.iter(), path))
                        }
                        _ => return Some((field.clone(), path)),
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
pub(crate) struct PathMetadata {
    field: Field,
    schema_path: SchemaPath,
    definition_level: DefinitionLevel,
    repetition_level: RepetitionLevel,
}

impl PathMetadata {
    fn new(schema: &Schema, field: &Field, schema_path: &SchemaPath) -> Self {
        let (definition_level, repetition_level) = Self::compute_levels(schema, schema_path);

        Self {
            field: field.clone(),
            schema_path: schema_path.clone(),
            definition_level,
            repetition_level,
        }
    }

    pub(crate) fn schema_path(&self) -> &[String] {
        &self.schema_path
    }

    pub(crate) fn field(&self) -> &Field {
        &self.field
    }

    fn compute_levels(schema: &Schema, schema_path: &SchemaPath) -> (u8, u8) {
        let mut definition_level = 0;
        let mut repetition_level = 0;

        let mut current_fields = schema.fields();
        for name in schema_path.iter() {
            let field = current_fields
                .iter()
                .find(|f| f.name() == name)
                .unwrap_or_else(|| {
                    panic!(
                        "Field with name: {} not found in path: {}",
                        name, schema_path
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
            "field:{} schema_path:{} max_def: {} max_rep: {}",
            self.field, self.schema_path, self.definition_level, self.repetition_level
        )
    }
}

#[derive(Debug)]
pub(crate) struct PathMetadataIterator<'a> {
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
            .map(|(field, schema_path)| PathMetadata::new(self.schema, &field, &schema_path))
    }
}

#[cfg(test)]
mod tests {
    use crate::field::DataType;
    use crate::field_path::{FieldPathIterator, PathMetadata};
    use crate::schema::{bool, integer, required_group, string, SchemaBuilder};
    use crate::schema_path::SchemaPath;

    #[test]
    fn test_empty_field_path_iterator() {
        let schema = SchemaBuilder::new("empty").build();
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 0);
    }

    #[test]
    fn test_basic_field_path_iterator() {
        let schema = SchemaBuilder::new("test")
            .field(integer("id"))
            .field(string("name"))
            .field(bool("active"))
            .build();

        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].1, SchemaPath::from(&["id"][..]));
        assert_eq!(paths[1].1, SchemaPath::from(&["name"][..]));
        assert_eq!(paths[2].1, SchemaPath::from(&["active"][..]));

        assert_eq!(paths[0].0.data_type(), &DataType::Integer);
        assert_eq!(paths[1].0.data_type(), &DataType::String);
        assert_eq!(paths[2].0.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_nested_field_path_iterator() {
        let schema = SchemaBuilder::new("test")
            .field(required_group(
                "user",
                vec![integer("id"), string("name"), bool("active")],
            ))
            .build();

        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].1, SchemaPath::from(&["user", "id"][..]));
        assert_eq!(paths[1].1, SchemaPath::from(&["user", "name"][..]));
        assert_eq!(paths[2].1, SchemaPath::from(&["user", "active"][..]));

        assert_eq!(paths[0].0.data_type(), &DataType::Integer);
        assert_eq!(paths[1].0.data_type(), &DataType::String);
        assert_eq!(paths[2].0.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_basic_field_path_metadata() {
        let schema = SchemaBuilder::new("test").field(integer("id")).build();
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();

        let id_metadata = PathMetadata::new(&schema, &paths[0].0, &paths[0].1);

        assert_eq!(*id_metadata.field(), integer("id"));
        assert_eq!(id_metadata.schema_path(), &["id"]);
        assert_eq!(id_metadata.definition_level, 0);
        assert_eq!(id_metadata.repetition_level, 0);
    }

    #[test]
    fn test_nested_field_path_metadata() {
        let schema = SchemaBuilder::new("test")
            .field(required_group("user", vec![string("name")]))
            .build();
        let paths = FieldPathIterator::new(&schema).collect::<Vec<_>>();

        let path_metadata = PathMetadata::new(&schema, &paths[0].0, &paths[0].1);

        assert_eq!(*path_metadata.field(), string("name"));
        assert_eq!(path_metadata.schema_path(), &["user", "name"]);
        assert_eq!(path_metadata.definition_level, 0);
        assert_eq!(path_metadata.repetition_level, 0);
    }
}
