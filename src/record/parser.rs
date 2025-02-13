use crate::record::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::record::value::{matches_struct, DepthFirstValueIterator};
use crate::record::{DataType, Field, PathVector, PathVectorExt, Schema, Value};
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
    state: ValueParserState,
}

struct ListContext {
    field_name: String,
    length: usize,
    current_index: usize,
}

impl ListContext {
    // TODO: Do not allow creating a list context with length zero
    fn new(field_name: String, length: usize) -> Self {
        Self {
            field_name,
            length,
            current_index: 0,
        }
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn position(&self) -> usize {
        self.current_index
    }

    pub fn increment(&mut self) {
        self.current_index += 1;
    }

    pub fn is_exhausted(&self) -> bool {
        self.current_index >= self.length
    }
}

#[derive(Default)]
struct ValueParserState {
    struct_stack: Vec<Vec<Field>>,
    list_stack: Vec<ListContext>,
    prev_path: PathVector,
}

impl<'a> ValueParser<'a> {
    fn new(schema: &'a Schema, value_iter: DepthFirstValueIterator<'a>) -> Self {
        let paths = PathMetadataIterator::new(schema).collect::<Vec<_>>();
        let state = if schema.is_empty() {
            ValueParserState::default()
        } else {
            let fields = schema
                .fields()
                .iter()
                .map(|f| f.clone())
                .collect::<Vec<_>>();
            ValueParserState {
                struct_stack: vec![fields],
                list_stack: vec![],
                prev_path: PathVector::default(),
            }
        };

        Self {
            schema,
            paths,
            value_iter,
            state,
        }
    }

    fn current_fields(&self) -> Option<&Vec<Field>> {
        self.state.struct_stack.last()
    }

    fn find_field_by(&self, name: &str) -> Option<&Field> {
        self.current_fields()
            .and_then(|fields| fields.iter().find(|f| f.name() == name))
    }

    fn current_list(&self) -> Option<&ListContext> {
        self.state.list_stack.last()
    }
}

#[derive(Debug)]
pub struct StripedColumnValue {
    value: Value,
    repetition_level: RepetitionLevel,
    definition_level: DefinitionLevel,
}

impl StripedColumnValue {
    fn new(
        value: Value,
        repetition_level: RepetitionLevel,
        definition_level: DefinitionLevel,
    ) -> Self {
        StripedColumnValue {
            value,
            repetition_level,
            definition_level,
        }
    }
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
            println!("~~~");
            println!("top-level: {}", path.is_empty());
            println!("value: {}", value);
            println!(
                "path transition {} to {}",
                self.state.prev_path.format(),
                path.format()
            );

            // Type-checking for top-level struct value
            if path.is_empty() {
                if let Value::Struct(named_values) = value {
                    if let Some(fields) = self.current_fields() {
                        if matches_struct(named_values, &fields) {
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

            // When backtracking in depth-first traversal, and on exiting a struct value it needs
            // to be removed from the stack.
            //
            // For example the previous path is `a.b.c.d` and now has transitioned to `a.x`. To
            // find the field definition for `x` we need to remove both `c` and `d` from the stack.
            //
            // From comparing the paths the common prefix is `a`. Both `b` and `x` are children
            // and were added to the stack together. So we need to pop the stack twice to remove
            // `c` and `d`.
            if path.len() < self.state.prev_path.len() {
                // backtracking in depth-first traversal
                let longest_common_prefix = path.longest_common_prefix(&self.state.prev_path);
                let pop_count = self.state.prev_path.len() - longest_common_prefix.len() - 1;
                for _ in 0..pop_count {
                    if let Some(fields) = self.state.struct_stack.pop() {
                        println!("Popped struct stack: {:?}", fields);
                    } else {
                        todo!("No items in struct stack to pop. This is bad!")
                    }
                }
            }

            // Updates transitioned path in state for computing prefix for the next value to
            // determine stack maintenance.
            self.state.prev_path = path.clone();

            if let Some(field_name) = path.last() {
                if let Some(field) = self.find_field_by(field_name).map(|field| field.clone()) {
                    if value.matches_type_shallow(&field) {
                        match value {
                            Value::Boolean(v) => {
                                return Some(Ok(StripedColumnValue::new(Value::Boolean(*v), 0, 0)))
                            }
                            Value::Integer(v) => {
                                return Some(Ok(StripedColumnValue::new(Value::Integer(*v), 0, 0)))
                            }
                            Value::String(v) => {
                                return Some(Ok(StripedColumnValue::new(
                                    Value::String(v.clone()),
                                    0,
                                    0,
                                )))
                            }
                            Value::List(items) => {
                                // Repeated values in a list container.
                                //
                                // The value iterator will yield the list items one by one. To
                                // keep track that we are within a list context across multiple
                                // `next()` calls of this iterator the state is maintained in a
                                // stack.
                                //
                                // A stack allows us to maintain context for arbitrary nesting. A
                                // list field type maybe a struct, and it can contain a field which
                                // is a list and so on.
                                if !items.is_empty() {
                                    self.state.list_stack.push(ListContext::new(
                                        field_name.to_string(),
                                        items.len(),
                                    ));
                                    continue;
                                } else {
                                    // Do not track state for an empty list. Here we return a scalar
                                    // column value immediately by examining the PathMetadata which
                                    // partially matches this path, to return the column or columns
                                    // which are terminated because this list is empty.
                                    //
                                    // It can be multiple column values because the list inner type
                                    // could be a struct with multiple fields. In the case of scalar
                                    // list type we only need to emit a single null value with that
                                    // item type here.
                                    todo!("get leaf field datatype from path metadata and return column value")
                                }
                            }
                            Value::Struct(_) => match field.data_type().clone() {
                                DataType::Struct(fields) => {
                                    let field_names = fields
                                        .iter()
                                        .map(|f| f.name())
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    println!("Adding {} to state.struct_stack", field_names);
                                    let cloned_fields =
                                        fields.iter().map(|f| f.clone()).collect::<Vec<_>>();
                                    self.state.struct_stack.push(cloned_fields);
                                    continue;
                                }
                                _ => unreachable!("expected struct field"),
                            },
                        }
                    } else if matches!(field.data_type(), DataType::List(_)) {
                        println!("expecting list item {}", value);
                        // We are in a list context and this is a list item.
                        //
                        // Shallow type checking is lazy and does not type check the contents of
                        // a list. This is necessary as lists may contain structs, and have
                        // arbitrary levels of nesting.
                        //
                        // Here we know the list item type, and also track state about the progress
                        // we have made in the list. We know the index position of this list item
                        // in the list container.
                        if let Some(list_context) = self.current_list() {
                            if field_name == list_context.field_name() {
                                match field.data_type() {
                                    DataType::List(item_type) => {
                                        match (value, item_type.as_ref()) {
                                            (Value::Boolean(v), DataType::Boolean) => {
                                                if !field.is_optional() && v.is_none() {
                                                    todo!("list item is not nullable")
                                                }
                                                return Some(Ok(StripedColumnValue::new(
                                                    Value::Boolean(*v),
                                                    0,
                                                    0,
                                                )));
                                            }
                                            (Value::Integer(v), DataType::Integer) => {
                                                if !field.is_optional() && v.is_none() {
                                                    todo!("list item is not nullable")
                                                }
                                                return Some(Ok(StripedColumnValue::new(
                                                    Value::Integer(*v),
                                                    0,
                                                    0,
                                                )));
                                            }
                                            (Value::String(v), DataType::String) => {
                                                if !field.is_optional() && v.is_none() {
                                                    todo!("list item is not nullable")
                                                }
                                                return Some(Ok(StripedColumnValue::new(
                                                    Value::String(v.clone()),
                                                    0,
                                                    0,
                                                )));
                                            }
                                            (
                                                Value::Struct(named_values),
                                                DataType::Struct(fields),
                                            ) => {
                                                todo!("handle nested struct")
                                            }
                                            _ => todo!("value type does not match data type"),
                                        }
                                    }
                                    _ => unreachable!("expected a list item"),
                                }
                            } else {
                                todo!("list context mismatch")
                            }
                        } else {
                            todo!("missing list context")
                        }
                    } else {
                        todo!("failed shallow type checking")
                    }
                } else {
                    todo!("field not found in struct context")
                }
            }
        }

        // TODO: generate NULL values when top-level optional/repeated paths are missing
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::schema::{
        bool, integer, optional_group, optional_integer, optional_string, repeated_group,
        repeated_integer, required_group, string,
    };
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

    #[test]
    fn test_nested_struct() {
        let schema = SchemaBuilder::new("nested_struct", vec![])
            .field(required_group(
                "a",
                vec![
                    required_group("b", vec![required_group("c", vec![integer("d")])]),
                    optional_group("x", vec![optional_group("y", vec![integer("z")])]),
                ],
            ))
            .build();

        let value = ValueBuilder::new()
            .field(
                "a",
                ValueBuilder::new()
                    .field(
                        "b",
                        ValueBuilder::new()
                            .field("c", ValueBuilder::new().field("d", 1).build())
                            .build(),
                    )
                    .field(
                        "x",
                        ValueBuilder::new()
                            .field("y", ValueBuilder::new().field("z", 2).build())
                            .build(),
                    )
                    .build(),
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        println!("{schema}");
        println!("{value}");
        println!("{:?}", &parsed);

        assert_eq!(parsed.len(), 1);

        // name column
        assert_eq!(parsed[0].value, Value::Integer(Some(1)));
        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[0].repetition_level, 0);
    }
}
