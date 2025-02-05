use crate::record::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::record::value::DepthFirstValueIterator;
use crate::record::{DataType, Field, Schema, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ParseError<'a> {
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
            matches!(field.data_type(), DataType::List(_)) || field.is_nullable(),
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
    path_metadata_map: HashMap<Vec<String>, PathMetadata<'a>>,
    value_iter: DepthFirstValueIterator<'a>,
    level_context: LevelContext,
}

impl<'a> ValueParser<'a> {
    fn new(schema: &'a Schema, value_iter: DepthFirstValueIterator<'a>) -> Self {
        let path_metadata_map = PathMetadataIterator::new(schema)
            .map(|path_metadata| (path_metadata.path().to_vec(), path_metadata))
            .collect();
        Self {
            schema,
            path_metadata_map,
            value_iter,
            level_context: Default::default(),
        }
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

    /**
    Iterate through the Value in depth-first order and when we reach the leaf
    node emit a StripedColumnValue.

    The schema is used to verify that the Value has the same path structure
    as defined. The type of each Value node is verified against the field
    definition in the schema (required | optional | repeated).

    If the value cannot be parsed abort processing and return a meaningful
    error message. (strict mode)

    If a path contains repeated or optional fields, the path may terminate
    before reaching the leaf node. In this case we return NULL as the value
    in StripedColumnValue.

    During Value traversal we maintain the definition level state. It is
    incremented whenever we read a Value node which is defined as either
    optional or repeated in the schema.

    The repetition level state is trickier. We track both the depth and
    the repetition level for repeated Values. Here depth does not mean
    the depth of a Value node in the tree, rather it is the repetition
    depth. The repetition level for each Value node can be computed using
    the previous (depth, repetition level) values.
        - The first item is a special case. The computed repetition level
        will be same as the repetition level of its parent.
        - For the remaining items the repetition level is same as the
        repetition depth of its parent.
        - For an empty list the computed repetition level will be same
        as the repetition level of its parent.

    The PathMetadata is a precomputed struct which contains the maximum
    possible values for definition levels and repetition levels for a
    given path. This allows us to implement bounds checking for computed
    level values:
        - The computed levels should not exceed the max value
        - The repetition level should not exceed definition level
        - The definition level begins at 1
        - The repetition level begins at 0

    Algorithm:
        for each (Value, PathVector) in DepthFirstValueIterator
            compute levels and update state:
                compute definition levels
                compute repetition levels
            validate Value:
                check datatype matches                  # type error
                check nullable matches                  # required/optional
                check name matches (only struct fields) # path error
                check bounds:
                    is within max definition level
                    is within max repetition level
                    is repetition level < definition level
            if primitive value
                return Ok(StripedColumnValue::new(value, rep, def))
            else    # early path termination
                return Ok(StripedColumnValue::new(null_value, rep, def))
    */
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
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
