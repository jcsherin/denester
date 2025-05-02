use crate::field::{DataType, Field};
use crate::schema::Schema;
use crate::schema_path::SchemaPath;
use std::slice::Iter;

#[derive(Debug)]
struct StructIterationState<'a> {
    field_iter: Iter<'a, Field>,
    schema_path: SchemaPath,
}

impl<'a> StructIterationState<'a> {
    fn new(field_iter: Iter<'a, Field>, schema_path: SchemaPath) -> Self {
        Self {
            field_iter,
            schema_path,
        }
    }
}

pub(crate) struct SchemaLeafIterator<'a> {
    stack: Vec<StructIterationState<'a>>,
}

impl<'a> SchemaLeafIterator<'a> {
    pub(crate) fn new(schema: &'a Schema) -> Self {
        Self {
            stack: vec![StructIterationState::new(
                schema.fields().iter(),
                SchemaPath::default(),
            )],
        }
    }
}

impl Iterator for SchemaLeafIterator<'_> {
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
        while let Some(struct_iter) = self.stack.last_mut() {
            if let Some(field) = struct_iter.field_iter.next() {
                let mut path = struct_iter.schema_path.clone();
                path.push(field.name().to_string());

                match field.data_type() {
                    DataType::Boolean | DataType::Integer | DataType::String => {
                        return Some((field.clone(), path))
                    }
                    DataType::List(datatype) => match datatype.as_ref() {
                        DataType::Struct(fields) => self
                            .stack
                            .push(StructIterationState::new(fields.iter(), path)),
                        _ => return Some((field.clone(), path)),
                    },
                    DataType::Struct(fields) => self
                        .stack
                        .push(StructIterationState::new(fields.iter(), path)),
                }
            } else {
                self.stack.pop();
            }
        }

        None
    }
}
#[cfg(test)]
mod tests {
    use crate::field::DataType;
    use crate::schema::test_utils::create_doc;
    use crate::schema::{bool, integer, required_group, string};
    use crate::schema_iter::SchemaLeafIterator;
    use crate::schema_path::SchemaPath;
    use crate::SchemaBuilder;

    #[test]
    fn test_empty_schema() {
        let schema = SchemaBuilder::new("empty").build();
        let paths = SchemaLeafIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 0);
    }

    #[test]
    fn test_flat_struct() {
        let schema = SchemaBuilder::new("test")
            .field(integer("id"))
            .field(string("name"))
            .field(bool("active"))
            .build();

        let paths = SchemaLeafIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].1, SchemaPath::from(&["id"][..]));
        assert_eq!(paths[1].1, SchemaPath::from(&["name"][..]));
        assert_eq!(paths[2].1, SchemaPath::from(&["active"][..]));

        assert_eq!(paths[0].0.data_type(), &DataType::Integer);
        assert_eq!(paths[1].0.data_type(), &DataType::String);
        assert_eq!(paths[2].0.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_nested_struct() {
        let schema = SchemaBuilder::new("test")
            .field(required_group(
                "user",
                vec![integer("id"), string("name"), bool("active")],
            ))
            .build();

        let paths = SchemaLeafIterator::new(&schema).collect::<Vec<_>>();
        assert_eq!(paths.len(), 3);

        assert_eq!(paths[0].1, SchemaPath::from(&["user", "id"][..]));
        assert_eq!(paths[1].1, SchemaPath::from(&["user", "name"][..]));
        assert_eq!(paths[2].1, SchemaPath::from(&["user", "active"][..]));

        assert_eq!(paths[0].0.data_type(), &DataType::Integer);
        assert_eq!(paths[1].0.data_type(), &DataType::String);
        assert_eq!(paths[2].0.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_field_path_iterator() {
        let doc = create_doc();
        let paths = SchemaLeafIterator::new(&doc).collect::<Vec<_>>();

        assert_eq!(paths.len(), 6);

        assert_eq!(paths[0].1, SchemaPath::from(&["DocId"][..]));
        assert_eq!(paths[1].1, SchemaPath::from(&["Links", "Backward"][..]));
        assert_eq!(paths[2].1, SchemaPath::from(&["Links", "Forward"][..]));
        assert_eq!(
            paths[3].1,
            SchemaPath::from(&["Name", "Language", "Code"][..])
        );
        assert_eq!(
            paths[4].1,
            SchemaPath::from(&["Name", "Language", "Country"][..])
        );
        assert_eq!(paths[5].1, SchemaPath::from(&["Name", "Url"][..]));

        assert_eq!(paths[0].0.name(), "DocId");
        assert_eq!(paths[1].0.name(), "Backward");
        assert_eq!(paths[2].0.name(), "Forward");
        assert_eq!(paths[3].0.name(), "Code");
        assert_eq!(paths[4].0.name(), "Country");
        assert_eq!(paths[5].0.name(), "Url");
    }
}
