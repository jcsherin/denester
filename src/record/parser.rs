use crate::record::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::record::schema::DepthFirstSchemaIterator;
use crate::record::value::{matches_struct, DepthFirstValueIterator};
use crate::record::{DataType, Field, PathVector, Schema, Value};
use std::borrow::Cow;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ParseError<'a> {
    UnknownField {
        path: String,
        field_name: String,
    },
    RequiredFieldIsMissing {
        field_path: FieldPath<'a>,
    },
    RequiredFieldIsNull {
        field_path: FieldPath<'a>,
    },
    DataTypeMismatch {
        field_path: FieldPath<'a>,
        expected: &'a DataType,
        found: &'a DataType,
    },
    PathNotFoundInSchema {
        expected: FieldPath<'a>,
        found: Vec<String>,
    },
    DefinitionLevelOutOfBounds {
        path_metadata: PathMetadata<'a>,
        found: u8,
    },
    RepetitionLevelOutOfBounds {
        path_metadata: PathMetadata<'a>,
        found: u8,
    },
    Other {
        message: Cow<'a, str>,
        field_path: FieldPath<'a>,
        source: Option<Box<dyn Error + 'static>>,
    },
}

impl<'a> Display for ParseError<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnknownField { path, field_name } => {
                write!(f, "Field name: {} not found in path: {}", field_name, path)
            }
            ParseError::RequiredFieldIsMissing { field_path } => {
                write!(f, "Required field is missing: {}", field_path)
            }
            ParseError::RequiredFieldIsNull { field_path } => {
                write!(f, "Required field is null: {}", field_path)
            }
            ParseError::DataTypeMismatch {
                field_path,
                expected,
                found,
            } => {
                write!(
                    f,
                    "Data type mismatch: expected {}, found {} at {}",
                    expected, found, field_path
                )
            }
            ParseError::PathNotFoundInSchema { expected, found } => {
                write!(
                    f,
                    "Path not found: {}, but expected {}",
                    found.join("."),
                    expected
                )
            }
            ParseError::DefinitionLevelOutOfBounds {
                path_metadata,
                found,
            } => {
                write!(
                    f,
                    "Definition level is out of bound: expected {}, found definition level: {}",
                    path_metadata, found
                )
            }
            ParseError::RepetitionLevelOutOfBounds {
                path_metadata,
                found,
            } => {
                write!(
                    f,
                    "Repetition level is out of bound: expected {}, found repetition level: {}",
                    path_metadata, found
                )
            }
            ParseError::Other {
                message,
                field_path,
                source,
            } => {
                if let Some(err) = source {
                    write!(f, "{} at {}. source: {}", message, field_path, err)
                } else {
                    write!(f, "ParseError: {} at {}", message, field_path)
                }
            }
        }
    }
}

impl<'a> Error for ParseError<'a> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ParseError::Other { source, .. } => source.as_deref(),
            _ => None,
        }
    }
}

/**
Schema:
message Document {
  required int64 DocId;
  optional group Links {
    repeated int64 Backward;
    repeated int64 Forward; }
  repeated group Name {
    repeated group Language {
      required string Code;
      optional string Country; }
    optional string Url; }}

Columns:
1. rep=0 def=0 DocId
2. rep=1 def=2 Links.Backward
3. rep=1 def=2 Links.Forward
4. rep=2 def=2 Name.Language.Code
5. rep=2 def=3 Name.Language.Country
6. rep=1 def=2 Name.Url

Core Types
```
Schema { fields: Vec<Field> }
    - defines the schema for a nested value
Field { name: string, datatype: DataType, nullable: bool }
    - each field has a datatype and we know if it is required/optional
DataType { Boolean, Integer, String, List(Box<Datatype>), Struct<Vec<Field>> }
    - a list represents 'repeated' fields
    - nested schema is represented using List(_) and Struct(_) datatypes
    - a List(List(_)) nesting is illegal
    - a List(Struct(_)) is a repetition of structs
    - leaf nodes in schema has a primitive datatype which is either - Bool, Integer or String
Value { Boolean(Option<bool>), Integer(Option<i64>), String(Option<String>),
    List(Vec<Value>),
    Struct(Vec<(String, Value)>), }
    - corresponds 1:1 with DataType and used for constructing nested values
    - List(vec![List(_)]) is illegal similar to the DataType counterpart
    - List(vec![Struct(_)]) are repeating struct values
    - leaf nodes in a value is one of - Boolean(_), Integer(_) or String(_) variants
    - NULL values are represented like Boolean(None), Integer(None) or String(None)
    - Allows construction of deeply nested values
```

Derived Types
```
FieldPath { field: &Field, path: Vec<String> }
    - corresponds to a column
    - a path like ["a", "b", "c"] maps to 'a.b.c' in the value record
    - the field definition is that of the leaf node alone (in the above example 'c')
    - we do not store the field references of ancestors
FieldPathIterator
    - iterator which returns the columns from a Schema definition
PathMetadata { field_path: &FieldPath, max_def: u8, max_rep: u8 }
    - the column FieldPath reference
    - maximum bounds for definition level (repeated + optional fields in path)
    - maximum bounds for repetition level (repeated fields in path)
PathMetadataIterator
    - composed from FieldPathIterator to return PathMetadata for all columns in Schema
```

Parser Types
```
ParseError { RequiredFieldIsMissing {..}, RequiredFieldIsNull {..}, .. }
    - categories of errors which can happen while parsing a Value instance
DepthFirstValueIterator { Item = &Value }
    - a depth-first iterator for Value which returns the nodes in the value tree
    - Value::List(_) implies repeated values and is treated as a special case
        - the node is not returned, but contents are added to the internal stack
```
*/

type DefinitionLevel = u8;
type RepetitionLevel = u8;
type RepetitionDepth = u8;

#[derive(Default)]
struct LevelContext {
    definition_level: DefinitionLevel,
    repetition_depth: RepetitionDepth,
    repetition_level: RepetitionLevel,
}

impl LevelContext {
    fn with_field(&self, field: &Field) -> Self {
        let increment = DefinitionLevel::from(
            matches!(field.data_type(), DataType::List(_)) || field.is_optional(),
        );

        Self {
            definition_level: self.definition_level + increment,
            repetition_depth: self.repetition_depth,
            repetition_level: self.repetition_level,
        }
    }
}

pub struct ValueParser<'a> {
    schema: &'a Schema,
    paths: Vec<PathMetadata<'a>>,
    value_iter: DepthFirstValueIterator<'a>,
    state: ValueParserState<'a>,
}

#[derive(Default)]
struct ValueParserState<'a> {
    schema_context: Vec<&'a [Field]>,
}

impl<'a> ValueParser<'a> {
    fn new(schema: &'a Schema, value_iter: DepthFirstValueIterator<'a>) -> Self {
        let paths = PathMetadataIterator::new(schema).collect::<Vec<_>>();
        let state = if schema.is_empty() {
            ValueParserState::default()
        } else {
            ValueParserState {
                schema_context: vec![schema.fields()],
            }
        };

        Self {
            schema,
            paths,
            value_iter,
            state,
        }
    }

    fn current_fields(&self) -> Option<&[Field]> {
        self.state.schema_context.last().map(|v| *v)
    }

    fn find_field_by(&self, name: &str) -> Option<&Field> {
        self.current_fields()
            .and_then(|fields| fields.iter().find(|f| f.name() == name))
    }
}

pub struct StripedColumnValue {
    value: Value,
    repetition_level: RepetitionLevel,
    definition_level: DefinitionLevel,
}

type StripedColumnResult<'a> = Result<StripedColumnValue, ParseError<'a>>;
impl<'a> Iterator for ValueParser<'a> {
    type Item = StripedColumnResult<'a>;

    /// TODO: Enforce top-level Struct in Schema & Value
    /// Right now top-level is a collection of fields and therefore we are unable to do
    /// type-checking without additional checks. When top-level Struct is enforced for
    /// both value and schema then we can immediately do type checking because the
    /// Schema top-level datatype is always going to be a Struct and not just a loose
    /// collection of fields.
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((value, path)) = self.value_iter.next() {
            println!("top-level: {}", path.is_empty());
            println!("value: {}", value);
            println!("path: {}", path.join("."));
            println!("~~~");

            // Type-checking for top-level struct value
            if path.is_empty() {
                if let Value::Struct(named_values) = value {
                    if let Some(fields) = self.current_fields() {
                        if matches_struct(named_values, fields) {
                            continue;
                        } else {
                            todo!("handle type checking failed for top-level value")
                        }
                    } else {
                        todo!("handle missing field definitions")
                    }
                } else {
                    todo!("handle unexpected value type")
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::schema::{bool, integer, optional_integer, repeated_integer, string};
    use crate::record::value::ValueBuilder;
    use crate::record::SchemaBuilder;

    #[test]
    fn test_optional_field_contains_null() {
        let schema = SchemaBuilder::new("optional_field", vec![optional_integer("x")]).build();
        let value = ValueBuilder::new().optional_integer("x", None).build();
        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 1);

        assert_eq!(parsed[0].value, Value::Integer(None));
        assert_eq!(parsed[0].definition_level, 1);
        assert_eq!(parsed[0].repetition_level, 0);
    }

    #[test]
    fn test_optional_field_is_missing() {
        let schema = SchemaBuilder::new("optional_field", vec![optional_integer("x")]).build();
        let value = ValueBuilder::new().build();
        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 1);

        assert_eq!(parsed[0].value, Value::Integer(None));
        assert_eq!(parsed[0].definition_level, 0); // missing field
        assert_eq!(parsed[0].repetition_level, 0);
    }

    #[test]
    fn test_optional_field_contains_value() {
        let schema = SchemaBuilder::new("optional_field", vec![optional_integer("x")]).build();
        let value = ValueBuilder::new().optional_integer("x", Some(10)).build();
        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 1);

        assert_eq!(parsed[0].value, Value::Integer(Some(10)));
        assert_eq!(parsed[0].definition_level, 1);
        assert_eq!(parsed[0].repetition_level, 0);
    }

    #[test]
    fn test_required_field_contains_value() {
        let schema = SchemaBuilder::new("required_field", vec![integer("x")]).build();
        let value = ValueBuilder::new().field("x", 10).build();
        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 1);

        assert_eq!(parsed[0].value, Value::Integer(Some(10)));
        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[0].repetition_level, 0);
    }

    #[test]
    fn test_required_field_is_missing() {
        let schema = SchemaBuilder::new("required_field", vec![integer("x")]).build();
        let value = ValueBuilder::new().build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        assert!(matches!(parser.next().unwrap(),
                Err(ParseError::RequiredFieldIsMissing { field_path })
                if field_path.path() == &["x"] && field_path.field() == &integer("x")),);
        assert!(parser.next().is_none());
    }

    #[test]
    fn test_required_field_contains_null() {
        let schema = SchemaBuilder::new("required_field", vec![integer("x")]).build();
        let value = ValueBuilder::new().field("x", Value::Integer(None)).build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        assert!(matches!(parser.next().unwrap(),
            Err(ParseError::RequiredFieldIsNull { field_path })
            if field_path.path() == &["x"] && field_path.field() == &integer("x")
        ));
        assert!(parser.next().is_none());
    }

    #[test]
    fn test_repeated_field_is_empty() {
        let schema = SchemaBuilder::new("repeated_field", vec![repeated_integer("xs")]).build();
        let value = ValueBuilder::new()
            .repeated("xs", Vec::<Value>::new())
            .build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item = parser.next().unwrap();
        assert_eq!(item.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item.as_ref().unwrap().definition_level, 1);
        assert_eq!(item.as_ref().unwrap().repetition_level, 0);

        assert!(parser.next().is_none());
    }

    #[test]
    fn test_repeated_field_is_not_empty() {
        let schema = SchemaBuilder::new("repeated_field", vec![repeated_integer("xs")]).build();
        let value = ValueBuilder::new().repeated("xs", vec![1, 2, 3]).build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item1 = parser.next().unwrap();
        assert_eq!(item1.as_ref().unwrap().value, Value::Integer(Some(1)));
        assert_eq!(item1.as_ref().unwrap().definition_level, 1);
        assert_eq!(item1.as_ref().unwrap().repetition_level, 0);

        let item2 = parser.next().unwrap();
        assert_eq!(item2.as_ref().unwrap().value, Value::Integer(Some(2)));
        assert_eq!(item2.as_ref().unwrap().definition_level, 1);
        assert_eq!(item2.as_ref().unwrap().repetition_level, 0);

        let item3 = parser.next().unwrap();
        assert_eq!(item3.as_ref().unwrap().value, Value::Integer(Some(3)));
        assert_eq!(item3.as_ref().unwrap().definition_level, 1);
        assert_eq!(item3.as_ref().unwrap().repetition_level, 0);

        assert!(parser.next().is_none());
    }

    /// TODO: Is there a way to distinguish between the encoding of an `[]` vs `[null]`?
    #[test]
    fn test_repeated_field_contains_nulls() {
        let schema = SchemaBuilder::new("repeated_field", vec![repeated_integer("xs")]).build();
        let value = Value::Struct(vec![(
            "xs".to_string(),
            Value::List(vec![
                Value::Integer(None),
                Value::Integer(None),
                Value::Integer(None),
            ]),
        )]);
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item1 = parser.next().unwrap();
        assert_eq!(item1.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item1.as_ref().unwrap().definition_level, 1);
        assert_eq!(item1.as_ref().unwrap().repetition_level, 0);

        let item2 = parser.next().unwrap();
        assert_eq!(item2.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item2.as_ref().unwrap().definition_level, 1);
        assert_eq!(item2.as_ref().unwrap().repetition_level, 0);

        let item3 = parser.next().unwrap();
        assert_eq!(item3.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item3.as_ref().unwrap().definition_level, 1);
        assert_eq!(item3.as_ref().unwrap().repetition_level, 0);

        assert!(parser.next().is_none());
    }
    #[test]
    fn test_repeated_field_is_missing() {
        let schema = SchemaBuilder::new("repeated_field", vec![repeated_integer("xs")]).build();
        let value = ValueBuilder::new().build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item = parser.next().unwrap();
        assert_eq!(item.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item.as_ref().unwrap().definition_level, 1);
        assert_eq!(item.as_ref().unwrap().repetition_level, 0);

        assert!(parser.next().is_none());
    }

    #[test]
    fn test_simple_struct() {
        let schema = SchemaBuilder::new(
            "user",
            vec![
                string("name"),
                integer("id"),
                bool("enrolled"),
                repeated_integer("groups"),
            ],
        )
        .build();

        let value = ValueBuilder::new()
            .field("name", "Patricia")
            .field("id", 1001)
            .field("enrolled", true)
            .field("groups", vec![1, 2, 3])
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 6);

        // name column
        assert_eq!(parsed[0].value, Value::String(Some("Patricia".to_string())));
        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[0].repetition_level, 0);

        // id column
        assert_eq!(parsed[1].value, Value::Integer(Some(1001)));
        assert_eq!(parsed[1].definition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);

        // enrolled column
        assert_eq!(parsed[2].value, Value::Boolean(Some(true)));
        assert_eq!(parsed[2].definition_level, 0);
        assert_eq!(parsed[2].repetition_level, 0);

        // groups column - first entry
        assert_eq!(parsed[3].value, Value::Integer(Some(1)));
        assert_eq!(parsed[3].definition_level, 1);
        assert_eq!(parsed[3].repetition_level, 0);

        // groups column - second entry
        assert_eq!(parsed[4].value, Value::Integer(Some(2)));
        assert_eq!(parsed[4].definition_level, 1);
        assert_eq!(parsed[4].repetition_level, 0);

        // groups column - third entry
        assert_eq!(parsed[5].value, Value::Integer(Some(3)));
        assert_eq!(parsed[5].definition_level, 1);
        assert_eq!(parsed[5].repetition_level, 0);
    }
}
