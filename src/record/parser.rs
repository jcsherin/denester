use crate::record::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::record::parser::ParseError::RequiredFieldIsNull;
use crate::record::value::{DepthFirstValueIterator, TypeCheckError};
use crate::record::{DataType, Field, PathVector, PathVectorExt, Schema, Value};
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::fmt::{write, Display, Formatter};
use std::path::Path;

#[derive(Debug)]
pub enum ParseError<'a> {
    RequiredFieldIsNull {
        field_path: FieldPath,
    },
    UnexpectedTopLevelValue {
        value: &'a Value,
    },
    FieldsNotFound {
        value: &'a Value,
    },
    TypeCheckFailed {
        prop_names: Vec<String>,
        fields: Vec<Field>,
    },
    MissingLevelContext {
        path: PathVector,
    },
    MissingListContext {
        path: PathVector,
    },
    ListItemNonNullable {
        path: Vec<String>,
        field: DataType,
    },
    PathIsEmpty {
        value: Value,
    },
    UnknownField {
        field_name: String,
        value: Value,
    },
}

impl<'a> From<TypeCheckError> for ParseError<'a> {
    fn from(err: TypeCheckError) -> Self {
        match err {
            TypeCheckError::DataTypeMismatch { .. } => {
                todo!()
            }
            TypeCheckError::RequiredFieldIsNull { path, field } => RequiredFieldIsNull {
                field_path: FieldPath::new(field, path.to_vec()),
            },
            TypeCheckError::RequiredFieldsAreMissing { .. } => {
                todo!()
            }
            TypeCheckError::StructSchemaMismatch { .. } => {
                todo!()
            }
            TypeCheckError::StructDuplicateProperty { .. } => {
                todo!()
            }
            TypeCheckError::StructUnknownProperty { .. } => {
                todo!()
            }
        }
    }
}

impl<'a> Display for ParseError<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::RequiredFieldIsNull { field_path } => {
                write!(f, "Required field is null: {}", field_path)
            }
            ParseError::UnexpectedTopLevelValue { value } => {
                write!(f, "Expected top-level struct, found: {}", value)
            }
            ParseError::FieldsNotFound { value } => {
                write!(f, "Field definitions not found for value: {}", value)
            }
            ParseError::TypeCheckFailed { prop_names, fields } => {
                write!(
                    f,
                    "Type checking failed for props: {} with fields: {}",
                    prop_names.join(", "),
                    fields
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            ParseError::MissingLevelContext { path } | ParseError::MissingListContext { path } => {
                write!(
                    f,
                    "Level context missing for computing levels. Path: {}",
                    path.format()
                )
            }
            ParseError::ListItemNonNullable { path, field } => {
                write!(
                    f,
                    "This list does not allow null values. field: {}, path: {}",
                    field,
                    path.format()
                )
            }
            ParseError::PathIsEmpty { value } => {
                write!(f, "Path is empty: {}", value)
            }

            ParseError::UnknownField { field_name, value } => {
                write!(f, "Unknown field name \"{}\": {}", field_name, value)
            }
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

#[derive(Debug, Default)]
struct LevelContext {
    definition_level: DefinitionLevel,
    repetition_depth: RepetitionDepth,
    repetition_level: RepetitionLevel,
}

impl LevelContext {
    fn with_field(&self, field: &Field) -> Self {
        let is_optional = field.is_optional();
        let is_repeated = matches!(field.data_type(), DataType::List(_));

        let definition_level =
            self.definition_level + DefinitionLevel::from(is_optional || is_repeated);
        let repetition_depth = self.repetition_depth + RepetitionDepth::from(is_repeated);
        let repetition_level = self.repetition_level;

        Self {
            definition_level,
            repetition_depth,
            repetition_level,
        }
    }
}

pub struct ValueParser<'a> {
    schema: &'a Schema,
    paths: Vec<PathMetadata>,
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
    computed_levels: Vec<LevelContext>,
    missing_paths: VecDeque<PathMetadata>,
}

impl ValueParserState {
    pub fn new(schema: &Schema) -> Self {
        if schema.is_empty() {
            Self::default()
        } else {
            let fields = schema.fields().to_vec();

            Self {
                struct_stack: vec![fields],
                list_stack: vec![],
                prev_path: PathVector::default(),
                computed_levels: vec![LevelContext::default()],
                missing_paths: VecDeque::new(),
            }
        }
    }

    pub fn push_list(&mut self, field: &Field, len: usize) {
        self.list_stack
            .push(ListContext::new(field.name().to_string(), len));
    }

    pub fn peek_struct(&self) -> Option<&Vec<Field>> {
        self.struct_stack.last()
    }

    fn find_struct_field_by(&self, name: &str) -> Option<&Field> {
        self.peek_struct()
            .and_then(|fields| fields.iter().find(|f| f.name() == name))
    }
}

impl<'a> ValueParser<'a> {
    fn new(schema: &'a Schema, value_iter: DepthFirstValueIterator<'a>) -> Self {
        let paths = PathMetadataIterator::new(schema).collect::<Vec<_>>();
        let state = ValueParserState::new(schema);

        Self {
            schema,
            paths,
            value_iter,
            state,
        }
    }

    /// Maintains level context state for column striping
    ///
    /// Updates the level context which contains the definition level, repetition depth and the
    /// repetition level of the value node. A new level context is added to the stack during
    /// depth-first traversal. On backtracking the common ancestor path prefix is used to figure out
    /// how many level contexts needs to popped from the stack.
    ///
    /// TODO: refactor
    /// TODO: Result return type, handle errors
    fn update_level_context(
        &mut self,
        field: &Field,
        current_path: &PathVector,
        prev_path: &PathVector,
        common_prefix: &PathVector,
    ) {
        let curr = match self.state.computed_levels.last() {
            None => LevelContext::default().with_field(field),
            Some(prev) => prev.with_field(field),
        };

        if let Some(pop_count) = self.state.prev_path.len().checked_sub(common_prefix.len()) {
            if prev_path.is_empty() {
                // But transition from 'a' to 'b' should have something to pop.
                // Transitions from .top-level to 'a' on the other hand has an
                // empty stack and therefore nothing needs to be popped.
                println!(
                    "nothing to pop at the top-level transition {} to {}",
                    prev_path.format(),
                    current_path.format()
                );
            } else {
                println!(
                    "prev_path: {} computed_levels: {}, pop_count: {}",
                    prev_path.format(),
                    self.state.computed_levels.len(),
                    pop_count
                );
                for _ in 0..pop_count {
                    if let Some(popped) = self.state.computed_levels.pop() {
                        println!("{} Pop level context {:?}", current_path.format(), popped);
                    } else {
                        todo!("computed levels stack is empty!")
                    }
                }
            }
            println!("{} Push level context {:?}", current_path.format(), curr);
            self.state.computed_levels.push(curr);
        } else {
            todo!("level context pop count subtraction overflowed")
        }
    }

    fn get_column_from_scalar(
        &self,
        path: &PathVector,
        value: &Value,
    ) -> Result<StripedColumnValue, ParseError<'a>> {
        let level_context = self
            .state
            .computed_levels
            .last()
            .ok_or(ParseError::MissingLevelContext { path: path.clone() })?;

        Ok(StripedColumnValue::new(
            value.clone(),
            level_context.repetition_level,
            level_context.definition_level,
        ))
    }

    fn get_column_from_scalar_list(
        &self,
        path: &PathVector,
        field: &Field,
        value: &Value,
        repetition_level: RepetitionLevel,
        definition_level: DefinitionLevel,
    ) -> Result<StripedColumnValue, ParseError<'a>> {
        if !field.is_optional() && value.is_null() {
            return Err(ParseError::ListItemNonNullable {
                path: path.clone(),
                field: field.data_type().clone(),
            });
        }

        Ok(StripedColumnValue::new(
            value.clone(),
            repetition_level,
            definition_level,
        ))
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

#[derive(Debug, Default)]
struct MissingFields {
    names: Vec<String>,
}

impl MissingFields {
    /// Returns missing fields of a struct value
    ///
    /// Only optional and repeated fields are collected. If a required field is missing then the struct
    /// value has failed type checking.
    fn with_struct(props: &Vec<(String, Value)>, fields: &Vec<Field>) -> Self {
        let mut missing_fields = MissingFields::default();
        let present_fields = props
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<HashSet<_>>();

        for field in fields {
            if present_fields.contains(field.name()) {
                continue;
            }

            if !field.is_required() {
                missing_fields.names.push(field.name().to_string());
            }
        }

        missing_fields
    }

    fn names(&self) -> &[String] {
        self.names.as_slice()
    }
}

fn find_missing_paths(
    prefix: &PathVector,
    missing_fields: &MissingFields,
    paths: &Vec<PathMetadata>,
) -> Vec<PathMetadata> {
    let mut missing_paths = vec![];

    for field_name in missing_fields.names() {
        let path = prefix.append_name(field_name.to_string());

        for path_metadata in paths {
            if path.len() > path_metadata.len() {
                continue;
            }

            if path
                .iter()
                .zip(path_metadata.path().iter())
                .all(|(x, y)| x == y)
            {
                missing_paths.push(path_metadata.clone());
            }
        }
    }

    missing_paths
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

            // At the top-level since the path is empty, it is not possible to search for the field by
            // name. So we extract the struct properties, and all the field definitions in the context
            // stack, and perform a shallow structural type-checking.
            if path.is_top_level() {
                let props = match value {
                    Value::Struct(props) => props.to_vec(),
                    _ => return Some(Err(ParseError::UnexpectedTopLevelValue { value })),
                };
                let fields = match self.state.peek_struct() {
                    None => return Some(Err(ParseError::FieldsNotFound { value })),
                    Some(fields) => fields.to_vec(),
                };
                match Value::type_check_struct_shallow(&props, &fields) {
                    Ok(_) => {
                        let missing_fields = MissingFields::with_struct(&props, &fields);
                        let missing_paths = find_missing_paths(&path, &missing_fields, &self.paths);
                        if !missing_paths.is_empty() {
                            self.state.missing_paths = VecDeque::from(missing_paths); // removing from front is O(1)
                            println!("Adding missing paths: {:?}", self.state.missing_paths)
                        }
                        continue;
                    }
                    Err(_) => {
                        return Some(Err(ParseError::TypeCheckFailed {
                            prop_names: props.iter().map(|(name, _)| name.clone()).collect(),
                            fields: fields.iter().cloned().collect(),
                        }))
                    }
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
            let longest_common_prefix = path.longest_common_prefix(&self.state.prev_path);

            if path.len() < self.state.prev_path.len() {
                // backtracking in depth-first traversal
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
            let tmp_prev_path = self.state.prev_path.clone();
            self.state.prev_path = path.clone();

            let field = match path
                .last()
                .ok_or(ParseError::PathIsEmpty {
                    value: value.clone(),
                })
                .and_then(|field_name| {
                    self.state
                        .find_struct_field_by(field_name)
                        .ok_or(ParseError::UnknownField {
                            field_name: field_name.clone(),
                            value: value.clone(),
                        })
                }) {
                Ok(f) => f.clone(),
                Err(err) => {
                    return Some(Err(err));
                }
            };

            match value.type_check_shallow(&field, &path) {
                Ok(_) => {
                    self.update_level_context(
                        &field,
                        &path,
                        &tmp_prev_path,
                        &longest_common_prefix,
                    );

                    match value {
                        Value::Boolean(_) | Value::Integer(_) | Value::String(_) => {
                            return Some(self.get_column_from_scalar(&path, &value));
                        }
                        Value::List(items) if items.is_empty() => {
                            println!("path: {}", path.format());
                            for path_metadata in &self.paths {
                                println!("{}", path_metadata);
                            }
                            /// There are two possible states here:
                            ///  - scalar type list item (single column)
                            ///  - struct list item (one or more columns)
                            ///
                            /// TODO: handle struct list item (returning one or more columns)
                            match field.data_type() {
                                DataType::List(inner) => match inner.as_ref() {
                                    DataType::Boolean | DataType::Integer | DataType::String => {
                                        let null_value =
                                            Value::create_null_or_empty(inner.as_ref());
                                        return Some(
                                            self.get_column_from_scalar(&path, &null_value),
                                        );
                                    }
                                    DataType::List(_) => {
                                        unreachable!("empty list: nested list types not allowed")
                                    }
                                    DataType::Struct(_) => {
                                        todo!("handle null buffering for struct fields")
                                    }
                                },
                                _ => unreachable!("expected list value to a have list datatype"),
                            }
                        }
                        Value::List(items) => {
                            self.state.push_list(&field, items.len());
                            continue;
                        }
                        Value::Struct(_) => {
                            match field.data_type() {
                                DataType::Boolean
                                | DataType::Integer
                                | DataType::String
                                | DataType::List(_) => {
                                    unreachable!("expected struct {}", field)
                                }
                                DataType::Struct(fields) => {
                                    self.state.struct_stack.push(fields.to_vec())
                                }
                            }
                            continue;
                        }
                    }
                }
                Err(type_check_err) => match field.data_type() {
                    DataType::List(_) => {
                        println!("expecting list item {}", value);

                        let list_context = match self.state.list_stack.last_mut() {
                            None => {
                                return Some(Err(ParseError::MissingListContext {
                                    path: path.clone(),
                                }))
                            }
                            Some(ctx) => ctx,
                        };

                        let level_context = match self.state.computed_levels.last() {
                            None => {
                                return Some(Err(ParseError::MissingLevelContext {
                                    path: path.clone(),
                                }))
                            }
                            Some(ctx) => ctx,
                        };

                        // Extract repetition, definition levels before we advance the cursor to
                        // the next position in this list context.
                        let (repetition_level, definition_level) = if list_context.position() > 0 {
                            (
                                level_context.repetition_depth,
                                level_context.definition_level,
                            )
                        } else {
                            (
                                level_context.repetition_level,
                                level_context.definition_level,
                            )
                        };
                        list_context.increment();

                        // List context mismatch with current list
                        if field.name() != list_context.field_name() {
                            return Some(Err(ParseError::MissingListContext {
                                path: path.clone(),
                            }));
                        }

                        if list_context.is_exhausted() {
                            // Note: Safe to pop because we already extracted the repetition and
                            // definition levels for this list item.
                            self.state.list_stack.pop();
                        }

                        match field.data_type() {
                            DataType::List(item_type) => match (value, item_type.as_ref()) {
                                (Value::Boolean(_), DataType::Boolean)
                                | (Value::Integer(_), DataType::Integer)
                                | (Value::String(_), DataType::String) => {
                                    return Some(self.get_column_from_scalar_list(
                                        &path,
                                        &field,
                                        value,
                                        repetition_level,
                                        definition_level,
                                    ));
                                }
                                (Value::Struct(named_values), DataType::Struct(fields)) => {
                                    // TODO: duplication with `self.state.push_struct` which takes &Field param
                                    self.state.struct_stack.push(fields.to_vec())
                                }
                                _ => todo!("value type does not match data type"),
                            },
                            _ => unreachable!("expected a list item"),
                        }
                    }
                    _ => return Some(Err(ParseError::from(type_check_err))),
                },
            }
        }

        // At the top-level if there is at least one present field which is processed the computed
        // level stacks last level context will correspond to the last processed field. We do not
        // need to compute common prefix, or path transitions because we know this is the top-level.
        // The common prefix is going to be empty, and we need to pop exactly one level context.
        //
        // It is possible that top-level struct is empty and no fields were processed, so the guard
        // here ensures that we do not accidentally pop the top-level context.
        if self.state.computed_levels.len() > 1 {
            self.state.computed_levels.pop().unwrap();
        }

        // Handles missing fields in top-level
        // Emit null column value if there are missing paths in this level
        if !self.state.missing_paths.is_empty() {
            let missing_path = self.state.missing_paths.pop_front().unwrap();
            let data_type = missing_path.field().data_type();

            match data_type {
                DataType::Boolean | DataType::Integer | DataType::String => {
                    let null_value = Value::create_null_or_empty(data_type);
                    println!("returning null value for {}", missing_path);

                    return Some(self.get_column_from_scalar(
                        &PathVector::from_slice(missing_path.path()),
                        &null_value,
                    ));
                }
                DataType::List(inner) => match inner.as_ref() {
                    DataType::Boolean | DataType::Integer | DataType::String => {
                        let null_value = Value::create_null_or_empty(inner.as_ref());

                        return Some(self.get_column_from_scalar(
                            &PathVector::from_slice(missing_path.path()),
                            &null_value,
                        ));
                    }
                    DataType::List(_) => {
                        unreachable!("not allowed")
                    }
                    DataType::Struct(_) => {
                        unreachable!("leaf field cannot have a struct as list item")
                    }
                },
                DataType::Struct(_) => {
                    todo!("handle missing struct")
                }
            }
        }

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
    use std::fmt::format;

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
    fn test_top_level_optional_field_is_missing() {
        let schema = SchemaBuilder::new("optional_field", vec![optional_integer("x")]).build();
        let value = ValueBuilder::new().build();
        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value, Value::Integer(None));
        assert_eq!(parsed[0].definition_level, 0);
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

        // assert!(matches!(parser.next().unwrap(),
        //         Err(ParseError::RequiredFieldIsMissing { field_path })
        //         if field_path.path() == &["x"] && field_path.field() == &integer("x")),);
        assert!(parser.next().is_none());
    }

    #[test]
    fn test_required_field_contains_null() {
        let schema = SchemaBuilder::new("required_field", vec![integer("x")]).build();
        let value = ValueBuilder::new().field("x", Value::Integer(None)).build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        assert!(matches!(parser.next().unwrap(),
            Err(RequiredFieldIsNull { field_path })
            if field_path.path() == &["x"] && field_path.field() == &integer("x")
        ));
        // TODO: add a test which contains other fields after the required field
        assert!(parser.next().is_none());
    }

    #[test]
    fn test_repeated_scalar_field_is_empty() {
        let schema = SchemaBuilder::new("repeated_field", vec![repeated_integer("xs")]).build();
        let value = ValueBuilder::new()
            .repeated("xs", Vec::<Value>::new())
            .build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item = parser.next().and_then(Result::ok).unwrap();

        assert_eq!(item.value, Value::Integer(None));
        assert_eq!(item.repetition_level, 0);
        assert_eq!(item.definition_level, 1); // because the list is empty, not missing

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
        assert_eq!(item2.as_ref().unwrap().repetition_level, 1);

        let item3 = parser.next().unwrap();
        assert_eq!(item3.as_ref().unwrap().value, Value::Integer(Some(3)));
        assert_eq!(item3.as_ref().unwrap().definition_level, 1);
        assert_eq!(item3.as_ref().unwrap().repetition_level, 1);

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
        assert_eq!(item2.as_ref().unwrap().repetition_level, 1);

        let item3 = parser.next().unwrap();
        assert_eq!(item3.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item3.as_ref().unwrap().definition_level, 1);
        assert_eq!(item3.as_ref().unwrap().repetition_level, 1);

        assert!(parser.next().is_none());
    }
    #[test]
    fn test_top_level_repeated_field_is_missing() {
        let schema = SchemaBuilder::new("repeated_field", vec![repeated_integer("xs")]).build();
        let value = ValueBuilder::new().build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item = parser.next().unwrap();
        assert_eq!(item.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item.as_ref().unwrap().definition_level, 0);
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

        assert_eq!(parsed[0].value, Value::String(Some("Patricia".to_string())));
        assert_eq!(parsed[1].value, Value::Integer(Some(1001)));
        assert_eq!(parsed[2].value, Value::Boolean(Some(true)));
        assert_eq!(parsed[3].value, Value::Integer(Some(1)));
        assert_eq!(parsed[4].value, Value::Integer(Some(2)));
        assert_eq!(parsed[5].value, Value::Integer(Some(3)));

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 0);
        assert_eq!(parsed[2].definition_level, 0);
        assert_eq!(parsed[3].definition_level, 1);
        assert_eq!(parsed[4].definition_level, 1);
        assert_eq!(parsed[5].definition_level, 1);

        assert_eq!(parsed[3].repetition_level, 0);
        assert_eq!(parsed[4].repetition_level, 1);
        assert_eq!(parsed[5].repetition_level, 1);
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

        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].value, Value::Integer(Some(1)));
        assert_eq!(parsed[1].value, Value::Integer(Some(2)));

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 2);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
    }

    #[test]
    fn test_schema_links() {
        // message doc {
        //   optional group Links {
        //      repeated int Backward;
        //      repeated int Forward; }}
        let schema = SchemaBuilder::new("Doc", vec![])
            .field(optional_group(
                "Links",
                vec![repeated_integer("Backward"), repeated_integer("Forward")],
            ))
            .build();

        // { links:
        //      forward: [20, 40, 60] }
        let value = ValueBuilder::new()
            .field(
                "Links",
                ValueBuilder::new()
                    .repeated("Forward", vec![20, 40, 60])
                    .build(),
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        println!("{:?}", parsed);

        assert_eq!(parsed[0].value, Value::Integer(Some(20)));
        assert_eq!(parsed[1].value, Value::Integer(Some(40)));
        assert_eq!(parsed[2].value, Value::Integer(Some(60)));

        assert_eq!(parsed[0].definition_level, 2);
        assert_eq!(parsed[1].definition_level, 2);
        assert_eq!(parsed[2].definition_level, 2);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 1);
        assert_eq!(parsed[2].repetition_level, 1);

        // TODO: handle empty Backward list
        assert_eq!(parsed.len(), 4);
    }

    #[test]
    fn test_schema_code() {
        // message doc {
        //  repeated group Name {
        //    repeated group Language {
        //      required string Code; }}}
        let schema = SchemaBuilder::new(
            "doc",
            vec![repeated_group(
                "Name",
                vec![repeated_group("Language", vec![string("Code")])],
            )],
        )
        .build();

        // Name
        //  Language
        //    Code: 'en-us'
        //  Language
        //    Code: 'en'
        // Name
        // Name
        //  Language
        //    Code: 'en-gb'
        let value = ValueBuilder::new()
            .repeated(
                "Name",
                vec![
                    ValueBuilder::new() // 0
                        .repeated(
                            "Language",
                            vec![
                                ValueBuilder::new().field("Code", "en-us").build(),
                                ValueBuilder::new().field("Code", "en").build(),
                            ],
                        )
                        .build(),
                    ValueBuilder::new().build(), // 1
                    ValueBuilder::new() // 2
                        .repeated(
                            "Language",
                            vec![ValueBuilder::new().field("Code", "en-gb").build()],
                        )
                        .build(),
                ],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        // TODO: handle missing repeated fields
        assert_eq!(parsed.len(), 4);
    }

    #[test]
    fn test_top_level_missing_fields() {
        // message doc {
        //   required int a,
        //   optional int b,
        //   optional int c, }
        let schema = SchemaBuilder::new("doc", vec![])
            .field(integer("a"))
            .field(optional_integer("b"))
            .field(optional_integer("c"))
            .build();

        // { a: 1, c: 3 }
        let value = ValueBuilder::new().field("a", 1).field("c", 3).build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        // Present fields are emitted first
        assert_eq!(parsed[0].value, Value::Integer(Some(1)));
        assert_eq!(parsed[1].value, Value::Integer(Some(3)));
        // Missing fields are emitted last
        assert_eq!(parsed[2].value, Value::Integer(None));

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 1);
        assert_eq!(parsed[2].definition_level, 0); // because b is missing

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
        assert_eq!(parsed[2].repetition_level, 0);
    }

    #[test]
    fn test_nested_missing_fields() {
        // message doc {
        //  required group a {
        //      required int x
        //      optional int y },
        //  required int b }
        let schema = SchemaBuilder::new("doc", vec![])
            .field(required_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .field(integer("b"))
            .build();

        // { a: { x: 1 }, b: 2 }
        let value = ValueBuilder::new()
            .field("a", ValueBuilder::new().field("x", 1).build())
            .field("b", 2)
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        assert_eq!(parsed[0].value, Value::Integer(Some(1))); // a.x
        assert_eq!(parsed[1].value, Value::Integer(None)); // a.y
        assert_eq!(parsed[2].value, Value::Integer(Some(2))); // b

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 0);
        assert_eq!(parsed[2].definition_level, 0);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
        assert_eq!(parsed[2].repetition_level, 0);
    }

    #[test]
    fn test_multiple_nested_missing_fields() {
        // message doc {
        //  required group a {
        //      required int x
        //      optional int y },
        //  optional int b }
        let schema = SchemaBuilder::new("doc", vec![])
            .field(required_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .field(optional_integer("b"))
            .build();

        // { a: { x: 1 } }
        let value = ValueBuilder::new()
            .field("a", ValueBuilder::new().field("x", 1).build())
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        assert_eq!(parsed[0].value, Value::Integer(Some(1))); // a.x
        assert_eq!(parsed[1].value, Value::Integer(None)); // a.y
        assert_eq!(parsed[2].value, Value::Integer(None)); // b

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 0);
        assert_eq!(parsed[2].definition_level, 0);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
        assert_eq!(parsed[2].repetition_level, 0);
    }

    #[test]
    fn test_missing_struct() {
        // message doc {
        //  optional group a {
        //      required int x
        //      optional int y },
        //  required int b }
        let schema = SchemaBuilder::new("doc", vec![])
            .field(optional_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .field(integer("b"))
            .build();

        // { b: 1 }
        let value = ValueBuilder::new().field("b", 1).build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 3);

        assert_eq!(parsed[0].value, Value::Integer(Some(1))); // b
        assert_eq!(parsed[1].value, Value::Integer(None)); // a.x
        assert_eq!(parsed[2].value, Value::Integer(None)); // a.y

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 0);
        assert_eq!(parsed[2].definition_level, 0);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
        assert_eq!(parsed[2].repetition_level, 0);
    }

    #[test]
    fn test_repeated_struct_with_missing_values() {
        // message doc {
        //  repeated group a {
        //      required int x;
        //      optional int y; }}
        let schema = SchemaBuilder::new("doc", vec![])
            .field(repeated_group(
                "a",
                vec![integer("x"), optional_integer("y")],
            ))
            .build();

        // { a: [{x: 1}, {x: 2, y: 3}
        let value = ValueBuilder::new()
            .field(
                "a",
                vec![
                    ValueBuilder::new().field("x", 1).build(),
                    ValueBuilder::new().field("x", 2).field("y", 3).build(),
                ],
            )
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 4);

        assert_eq!(parsed[0].value, Value::Integer(Some(1)));
        assert_eq!(parsed[1].value, Value::Integer(None));
        assert_eq!(parsed[2].value, Value::Integer(Some(2)));
        assert_eq!(parsed[3].value, Value::Integer(Some(3)));

        assert_eq!(parsed[0].definition_level, 1);
        assert_eq!(parsed[1].definition_level, 1);
        assert_eq!(parsed[2].definition_level, 1);
        assert_eq!(parsed[3].definition_level, 2);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
        assert_eq!(parsed[2].repetition_level, 1);
        assert_eq!(parsed[3].repetition_level, 1);
    }

    #[test]
    fn test_backtrack_struct_siblings() {
        // message doc {
        //  required group a {
        //      optional int x },
        //  required group b {
        //      optional int y }}
        let schema = SchemaBuilder::new("doc", vec![])
            .field(required_group("a", vec![optional_integer("x")]))
            .field(required_group("b", vec![optional_integer("y")]))
            .build();

        // { a: {}, b: {} }
        let value = ValueBuilder::new()
            .field("a", ValueBuilder::new().build())
            .field("b", ValueBuilder::new().build())
            .build();

        let parser = ValueParser::new(&schema, value.iter_depth_first());
        let parsed = parser
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].value, Value::Integer(None));
        assert_eq!(parsed[1].value, Value::Integer(None));

        assert_eq!(parsed[0].definition_level, 0);
        assert_eq!(parsed[1].definition_level, 0);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
    }

    #[test]
    fn test_deep_nesting_missing_fields() {
        // message doc {
        //  optional group a {
        //      required group b {
        //          optional group c {
        //             required int x;
        //             optional int y; }}}}
        let schema = SchemaBuilder::new("doc", vec![])
            .field(optional_group(
                "a",
                vec![required_group(
                    "b",
                    vec![optional_group(
                        "c",
                        vec![integer("x"), optional_integer("y")],
                    )],
                )],
            ))
            .build();

        // Value: { a: { b: { c: { x: 1 } } } }
        let value = ValueBuilder::new()
            .field(
                "a",
                ValueBuilder::new()
                    .field(
                        "b",
                        ValueBuilder::new()
                            .field("c", ValueBuilder::new().field("x", 1).build())
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

        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].value, Value::Integer(Some(1)));
        assert_eq!(parsed[1].value, Value::Integer(None));

        assert_eq!(parsed[0].definition_level, 2);
        assert_eq!(parsed[1].definition_level, 2);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 0);
    }
}
