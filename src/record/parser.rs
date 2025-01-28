use crate::record::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::record::value::DepthFirstValueIterator;
use crate::record::{DataType, Schema, Value};
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
pub struct ValueParser<'a> {
    schema: &'a Schema,
    path_metadata_map: HashMap<Vec<String>, PathMetadata<'a>>,
    value_iter: DepthFirstValueIterator<'a>,
    current_definition_level: u8,
    previous_repetition_level: u8,
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
            current_definition_level: 0,
            previous_repetition_level: 0,
        }
    }
}

pub struct StripedColumnValue {
    value: Value,
    repetition_level: u8,
    definition_level: u8,
}

type StripedColumnResult<'a> = Result<StripedColumnValue, ParseError<'a>>;
impl<'a> Iterator for ValueParser<'a> {
    type Item = StripedColumnResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
        // for (value, path_vector) in value_iter
        //      validate value against schema using path_vector
        //          abort if parse error
        //      compute definition & repetition levels
        //      validate levels are within known bounds using path_metadata
        //
        //      if node matches leaf field described in path_metadata
        //          emit StripedColumnValue
        //      else path is missing or terminated early
        //          emit NULL StripedColumnValue
    }
}
