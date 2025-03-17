use crate::record::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::record::parser::ParseError::RequiredFieldIsNull;
use crate::record::value::{DepthFirstValueIterator, TypeCheckError};
use crate::record::{DataType, Field, PathVector, PathVectorExt, PathVectorSlice, Schema, Value};
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter::Peekable;
use std::ops::Deref;

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
    MissingStructContext {
        path: Vec<String>,
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
            ParseError::MissingStructContext { path } => {
                write!(
                    f,
                    "Missing struct field definitions for path: {}",
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

type DefinitionLevel = u8;
type RepetitionLevel = u8;
type RepetitionDepth = u8;

#[derive(Debug)]
struct LevelContext {
    definition_level: DefinitionLevel,
    repetition_depth: RepetitionDepth,
    repetition_level: RepetitionLevel,
    path: PathVector,
}

impl Default for LevelContext {
    fn default() -> Self {
        Self {
            definition_level: 0,
            repetition_depth: 0,
            repetition_level: 0,
            path: PathVector::root(),
        }
    }
}

impl LevelContext {
    fn with_field(&self, field: &Field, path: &PathVector) -> Self {
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
            path: path.clone(),
        }
    }

    fn path(&self) -> &PathVector {
        &self.path
    }
}

pub struct ValueParser<'a> {
    schema: &'a Schema,
    paths: Vec<PathMetadata>,
    value_iter: Peekable<DepthFirstValueIterator<'a>>,
    state: ValueParserState,

    // Queues for handling values and missing paths in correct order
    work_queue: VecDeque<WorkItem<'a>>,
    missing_paths_queue: VecDeque<WorkItem<'a>>,
}

#[derive(Debug)]
struct ListContext {
    field_name: String,
    length: usize,
    current_index: usize,
    depth: usize,
}

impl ListContext {
    // TODO: Do not allow creating a list context with length zero
    fn new(field_name: String, length: usize, depth: usize) -> Self {
        Self {
            field_name,
            length,
            current_index: 0,
            depth,
        }
    }

    fn depth(&self) -> usize {
        self.depth
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

#[derive(Debug)]
struct DequeStack<T> {
    stack: Vec<VecDeque<T>>,
}

impl<T> DequeStack<T> {
    fn new() -> Self {
        let stack: Vec<VecDeque<T>> = vec![];
        Self { stack }
    }

    fn push_frame(&mut self, frame: Vec<T>) {
        if !frame.is_empty() {
            let frame = VecDeque::from(frame);
            self.stack.push(frame);
        }
    }

    fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }
}

impl<T> Default for DequeStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<Vec<T>> for DequeStack<T> {
    fn from(value: Vec<T>) -> Self {
        if value.is_empty() {
            Self::default()
        } else {
            Self {
                stack: vec![VecDeque::from(value)],
            }
        }
    }
}

impl<T> Iterator for DequeStack<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Removes all empty deques from top
        while self.stack.last().map_or(false, VecDeque::is_empty) {
            self.stack.pop();
        }

        // Get an item from first non-empty deque
        let item = self.stack.last_mut()?.pop_front();

        // Pop the deque if it is now empty
        if self.stack.last().map_or(false, VecDeque::is_empty) {
            self.stack.pop();
        }

        item
    }
}

#[derive(Debug, Default)]
struct StructContext {
    fields: Vec<Field>,
    path: PathVector,
}

impl StructContext {
    fn new(fields: Vec<Field>, path: PathVector) -> Self {
        Self {
            fields: fields,
            path: path,
        }
    }

    fn fields(&self) -> &[Field] {
        self.fields.as_slice()
    }

    fn path(&self) -> &PathVector {
        &self.path
    }

    fn find_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name() == name)
    }
}

#[derive(Debug)]
enum WorkItem<'a> {
    Value(&'a Value, PathVector),
    MissingValue(PathMetadata),
}

#[derive(Default)]
struct ValueParserState {
    struct_stack: Vec<StructContext>,
    list_stack: Vec<ListContext>,
    prev_path: PathVector,
    computed_levels: Vec<LevelContext>,
    missing_path_frames: DequeStack<PathMetadata>,
}

impl ValueParserState {
    pub fn new(schema: &Schema) -> Self {
        if schema.is_empty() {
            Self::default()
        } else {
            Self {
                struct_stack: vec![StructContext::new(
                    schema.fields().to_vec(),
                    PathVector::root(),
                )],
                list_stack: vec![],
                prev_path: PathVector::root(),
                computed_levels: vec![LevelContext::default()],
                missing_path_frames: DequeStack::new(),
            }
        }
    }

    pub fn push_list(&mut self, field: &Field, len: usize, path: &PathVector) {
        self.list_stack
            .push(ListContext::new(field.name().to_string(), len, path.len()));
    }

    pub fn peek_struct(&self) -> Option<&StructContext> {
        self.struct_stack.last()
    }

    fn find_field(&self, name: &str) -> Option<&Field> {
        self.peek_struct().and_then(|ctx| ctx.find_field(name))
    }

    /// Stack maintenance during backtracking in depth-first traversal
    fn handle_backtracking(&mut self, curr_path: PathVectorSlice) {
        // Traversal without backtracking. Proceed only if the path transitions to either a sibling
        // or ancestor.
        if curr_path.len() > self.prev_path.len() {
            return;
        }

        while self.computed_levels.len() > 1 {
            let top = self.computed_levels.last().unwrap();

            if top.path().is_root() || curr_path.starts_with(top.path()) {
                break;
            }

            self.computed_levels.pop().unwrap();
        }

        while self.struct_stack.len() > 1 {
            let top = self.struct_stack.last().unwrap();

            if top.path().is_root()
                || (top.path().len() < curr_path.len() && curr_path.starts_with(top.path()))
            {
                break;
            }

            self.struct_stack.pop().unwrap();
        }

        // TODO: add `depth` method to PathVector
        let curr_depth = curr_path.len();
        while !self.list_stack.is_empty() && self.list_stack.last().unwrap().depth > curr_depth {
            let popped = self.list_stack.pop();
            println!(
                "backtracking: popped list context at depth {}: {:#?}. current depth: {}",
                popped.as_ref().unwrap().depth,
                popped.as_ref().unwrap(),
                curr_depth,
            );
        }
    }
}

impl<'a> ValueParser<'a> {
    fn new(schema: &'a Schema, value_iter: DepthFirstValueIterator<'a>) -> Self {
        let paths = PathMetadataIterator::new(schema).collect::<Vec<_>>();
        let state = ValueParserState::new(schema);

        let mut value_iter = value_iter.peekable();
        let mut work_queue = VecDeque::new();
        let mut missing_paths_queue = VecDeque::new();

        if let Some((root_value, root_path)) = value_iter.next() {
            debug_assert!(root_path.is_root());

            if let Value::Struct(props) = root_value {
                if props.is_empty() && !schema.fields().is_empty() {
                    // Handle empty value by queueing missing paths now
                    let missing_paths =
                        find_missing_paths(&root_path, props, schema.fields(), &paths);
                    missing_paths_queue
                        .extend(missing_paths.into_iter().map(WorkItem::MissingValue))
                } else {
                    work_queue.push_back(WorkItem::Value(root_value, root_path));
                }
            }
        } else {
            unimplemented!("Handle the value iterator being empty before first use")
        }

        Self {
            schema,
            paths,
            value_iter,
            state,
            work_queue,
            missing_paths_queue,
        }
    }

        }
    }

    /// Queue missing paths for current struct level
    fn queue_missing_paths(&mut self) {
        if let Some(struct_fields_frame) = self.state.peek_struct() {
            if let Some((Value::Struct(props), path)) = self.value_iter.peek() {
                self.missing_paths_queue.extend(
                    find_missing_paths(path, props, struct_fields_frame.fields(), &self.paths)
                        .iter()
                        .map(|path_metadata| WorkItem::MissingValue(path_metadata.clone())),
                )
            }
        }
    }

    fn process_missing_path(&mut self, missing_path: &PathMetadata) -> StripedColumnResult {
        self.state.handle_backtracking(missing_path.path());
        self.state.prev_path = PathVector::from(missing_path.path());

        let data_type = missing_path.field().data_type();
        match data_type {
            DataType::Boolean | DataType::Integer | DataType::String => self
                .get_column_from_scalar(
                    &PathVector::from_slice(missing_path.path()),
                    &Value::create_null_or_empty(data_type),
                ),
            DataType::List(inner) => match inner.as_ref() {
                DataType::Boolean | DataType::Integer | DataType::String => self
                    .get_column_from_scalar(
                        &PathVector::from_slice(missing_path.path()),
                        &Value::create_null_or_empty(inner.as_ref()),
                    ),
                DataType::List(_) => {
                    unreachable!("not allowed")
                }
                DataType::Struct(_) => {
                    unreachable!("leaf field cannot have a struct as list item")
                }
            },
            DataType::Struct(_) => {
                unreachable!("leaf field cannot be a struct value")
            }
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
struct FieldName(String);

impl Deref for FieldName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for FieldName {
    fn from(s: String) -> Self {
        FieldName(s)
    }
}

#[derive(Debug, Default)]
struct MissingFields(Vec<FieldName>);

impl MissingFields {
    /// Returns missing fields of a struct value
    ///
    /// Only optional and repeated fields are collected. If a required field is missing then the struct
    /// value has failed type checking.
    fn with_struct(props: &Vec<(String, Value)>, fields: &[Field]) -> Self {
        let present_fields = props
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<HashSet<_>>();

        let missing_fields = fields
            .iter()
            .filter(|f| !present_fields.contains(f.name()) && !f.is_required())
            .map(|f| FieldName::from(f.name().to_string()))
            .collect();

        Self(missing_fields)
    }
}

impl Deref for MissingFields {
    type Target = [FieldName];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn find_missing_paths(
    prefix: &PathVector,
    props: &Vec<(String, Value)>,
    fields: &[Field],
    paths: &Vec<PathMetadata>,
) -> Vec<PathMetadata> {
    MissingFields::with_struct(&props, &fields)
        .iter()
        .flat_map(|field_name| {
            let path = prefix.append_name(field_name.to_string());
            paths
                .iter()
                .filter(move |path_metadata| path_metadata.path().starts_with(&path))
                .cloned()
        })
        .collect()
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
    ///
    /// # Queue Processing - A Sketch
    /// The current approach loops through the depth-first value iterator and returns a column
    /// value when it reaches the leaf of a path. The control flow contains explicit `continue`
    /// and implicit continuations to get the next item from the value iterator. These continuations
    /// make it hard to pause value iteration and handle the paths which are missing at the struct
    /// level.
    ///
    /// An imperative approach is to design a state machine for transitions. So if we have processed
    /// all the present fields, and if there are missing fields then the state is transitioned to
    /// processing missing paths before returning to value iteration state. A terminal state is also
    /// needed to handle all the remaining missing paths when the value iterator is exhausted.
    ///
    /// A better approach will be to decoupled traversal logic from column-striping processor using
    /// a queue to manage the order in which work is done. The existing depth-first traversal is
    /// used to fill the task queue. But if at a struct there are missing paths to process they are
    /// added to the queue. The processing of the items in the work queue is not tightly coupled
    /// to the order of traversal anymore.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(work_item) = self.work_queue.pop_front() {
            } else if let Some((value, path)) = self.value_iter.next() {
            } else if !self.missing_paths_queue.is_empty() {
            } else {
                return None;
            }
        }
        // The current control flow either returns a terminal column value or ends up in an
        // explicit/implicit continuation of the depth-first value iterator. This makes it quite
        // hard to handle missing paths at a value node level before resuming value traversal.
        //
        // There are the following cases to handle:
        //     - Iterate depth-first over value to return a column value on reaching the terminal
        // node in the path
        //     - If there are missing paths at a level, they need to processed after all present
        // properties at that level has been processed. Only then do we process the next value
        // in the iterator.
        //     - The value iterator is exhausted, and we process all the remaining paths.
        //
        // The traversal logic has to be decoupled from the processing fields logic for this code
        // to be easier to maintain. I considered implementing this as state machine transitions,
        // which could make this easier to understand. The control flow remains the same, and we
        // encode explicit state transitions at each step. The traversal and processing remains
        // tightly coupled.
        //
        // Instead, a queue can be added to the `ValueParserState`, and the work items will be
        // processed based on their priority. The highest priority is processing missing paths
        // for a level. And normal priority for processing a value. So enqueue missing path work
        // items once we have processed all the present fields in a level. The level transition,
        // specifically the backtracking to sibling or parent can be detected for enqueuing
        // missing paths.
        //
        // Implementation Sketch,
        // loop {
        //     1_handle_work_queue_item
        //         VisitValue => {
        //            // Handle backtracking
        //            // Save previous path to state
        //            // Match value by type
        //            //   Scalar types return a value immediately
        //            //   Empty list return a value immediately
        //            //   But if empty list type is a struct then we queue missing paths instead
        //            //   Otherwise for a non-empty list add to stack and continue
        //            //   For struct add to stack and continue
        //         }
        //         HandleMissingPath => { ... }
        //
        //     // work queue is empty so get the next value from iterator
        //     2_get_next_value_from_iterator
        //         if is_sibling_transition || backtracking_transition {
        //             queue_missing_paths
        //         }
        //         queue_value_visitor
        //
        //    // value iterator is exhausted so queue remaining missing paths
        //     3_queue_remaining_missing_paths
        //         while !struct_fields_stack.empty {
        //             pop_struct_fields_frame
        //             queue_missing_paths
        //         }
        // }
        //
        // // TODO: Handle missing required fields when depth-first value iterator is exhausted
        // while let Some((value, path)) = self.value_iter.next() {
        //     // At the top-level since the path is empty, it is not possible to search for the field by
        //     // name. So we extract the struct properties, and all the field definitions in the context
        //     // stack, and perform a shallow structural type-checking.
        //     if path.is_root() {
        //         let props = match value {
        //             Value::Struct(props) => props.to_vec(),
        //             _ => return Some(Err(ParseError::UnexpectedTopLevelValue { value })),
        //         };
        //         let fields = match self.state.peek_struct() {
        //             None => return Some(Err(ParseError::FieldsNotFound { value })),
        //             Some(ctx) => ctx.fields.to_vec(),
        //         };
        //         match Value::type_check_struct_shallow(&props, &fields) {
        //             Ok(_) => {
        //                 let missing_paths = find_missing_paths(&path, &props, &fields, &self.paths);
        //                 self.state.missing_path_frames = DequeStack::from(missing_paths);
        //                 continue;
        //             }
        //             Err(_) => {
        //                 return Some(Err(ParseError::TypeCheckFailed {
        //                     prop_names: props.iter().map(|(name, _)| name.clone()).collect(),
        //                     fields: fields.iter().cloned().collect(),
        //                 }))
        //             }
        //         }
        //     }
        //
        //     self.state.handle_backtracking(&path);
        //     let saved_prev_path = self.state.prev_path.clone();
        //     self.state.prev_path = path.clone();
        //
        //     println!("-----------");
        //     println!("{:#?} -> {:#?}", saved_prev_path.format(), path.format());
        //     println!("value: {:?}", value);
        //     println!(
        //         "active frame [struct]: {:?}",
        //         self.state.struct_stack.last().unwrap()
        //     );
        //     if self.state.struct_stack.len() >= 2 {
        //         println!(
        //             "parent frame [struct]: {:?}",
        //             self.state.struct_stack.iter().rev().nth(1).unwrap()
        //         );
        //     }
        //     println!("active frame [list]: {:?}", self.state.list_stack.last());
        //     if self.state.list_stack.len() >= 2 {
        //         println!(
        //             "parent frame [list]: {:?}",
        //             self.state.list_stack.iter().rev().nth(1).unwrap()
        //         );
        //     }
        //     println!(
        //         "active frame [level]: {:?}",
        //         self.state.computed_levels.last()
        //     );
        //     if self.state.computed_levels.len() >= 2 {
        //         println!(
        //             "parent frame [level]: {:?}",
        //             self.state.computed_levels.iter().rev().nth(1).unwrap()
        //         );
        //     }
        //     if !self.state.missing_path_frames.is_empty() {
        //         println!("missing_path_frames: {:?}", self.state.missing_path_frames);
        //     }
        //
        //     let field = match path
        //         .last()
        //         .ok_or(ParseError::PathIsEmpty {
        //             value: value.clone(),
        //         })
        //         .and_then(|field_name| {
        //             self.state
        //                 .find_field(field_name)
        //                 .ok_or(ParseError::UnknownField {
        //                     field_name: field_name.clone(),
        //                     value: value.clone(),
        //                 })
        //         }) {
        //         Ok(f) => f.clone(),
        //         Err(err) => {
        //             println!("Failed to find field {}", err);
        //             return Some(Err(err));
        //         }
        //     };
        //
        //     match value.type_check_shallow(&field, &path) {
        //         Ok(_) => {
        //             if let Some(parent_level_ctx) = self.state.computed_levels.last() {
        //                 self.state
        //                     .computed_levels
        //                     .push(parent_level_ctx.with_field(&field, &path))
        //             } else {
        //                 println!("Failed to find parent level ctx");
        //                 return Some(Err(ParseError::MissingLevelContext { path: path.clone() }));
        //             }
        //
        //             match value {
        //                 Value::Boolean(_) | Value::Integer(_) | Value::String(_) => {
        //                     let column_value = self.get_column_from_scalar(&path, &value);
        //                     println!("Column-striped value: {:?}", column_value);
        //                     return Some(column_value);
        //                 }
        //                 Value::List(items) if items.is_empty() => {
        //                     println!("Processing an empty list");
        //                     /// There are two possible states here:
        //                     ///  - scalar type list item (single column)
        //                     ///  - struct list item (one or more columns)
        //                     ///
        //                     /// TODO: handle struct list item (returning one or more columns)
        //                     match field.data_type() {
        //                         DataType::List(inner) => match inner.as_ref() {
        //                             DataType::Boolean | DataType::Integer | DataType::String => {
        //                                 let null_value =
        //                                     Value::create_null_or_empty(inner.as_ref());
        //                                 let column_value =
        //                                     self.get_column_from_scalar(&path, &null_value);
        //                                 println!("Column-striped value: {:?}", column_value);
        //                                 return Some(column_value);
        //                             }
        //                             DataType::List(_) => {
        //                                 unreachable!("empty list: nested list types not allowed")
        //                             }
        //                             DataType::Struct(_) => {
        //                                 println!("Null buffering for empty struct in list not implemented");
        //                                 todo!("handle null buffering for struct fields")
        //                             }
        //                         },
        //                         _ => unreachable!("expected list value to a have list datatype"),
        //                     }
        //                 }
        //                 Value::List(items) => {
        //                     println!("Adding a new frame to list stack");
        //                     self.state.push_list(&field, items.len(), &path);
        //                     continue;
        //                 }
        //                 Value::Struct(_) => {
        //                     println!("Adding a new frame to struct stack");
        //                     match field.data_type() {
        //                         DataType::Boolean
        //                         | DataType::Integer
        //                         | DataType::String
        //                         | DataType::List(_) => {
        //                             unreachable!("expected struct {}", field)
        //                         }
        //                         DataType::Struct(fields) => self
        //                             .state
        //                             .struct_stack
        //                             .push(StructContext::new(fields.to_vec(), path.clone())),
        //                     }
        //                     continue;
        //                 }
        //             }
        //         }
        //         Err(type_check_err) => match field.data_type() {
        //             DataType::List(_) => {
        //                 println!("Type check err: {:?}", type_check_err);
        //                 println!("Now processing a list item");
        //                 let list_context = match self.state.list_stack.last_mut() {
        //                     None => {
        //                         return Some(Err(ParseError::MissingListContext {
        //                             path: path.clone(),
        //                         }))
        //                     }
        //                     Some(ctx) => ctx,
        //                 };
        //
        //                 let level_context = match self.state.computed_levels.last() {
        //                     None => {
        //                         return Some(Err(ParseError::MissingLevelContext {
        //                             path: path.clone(),
        //                         }))
        //                     }
        //                     Some(ctx) => ctx,
        //                 };
        //
        //                 // Extract repetition, definition levels before we advance the cursor to
        //                 // the next position in this list context.
        //                 let (repetition_level, definition_level) = if list_context.position() > 0 {
        //                     (
        //                         level_context.repetition_depth,
        //                         level_context.definition_level,
        //                     )
        //                 } else {
        //                     (
        //                         level_context.repetition_level,
        //                         level_context.definition_level,
        //                     )
        //                 };
        //                 list_context.increment();
        //                 println!("list index position incremented: {:?}", list_context);
        //
        //                 // List context mismatch with current list
        //                 if field.name() != list_context.field_name() {
        //                     println!("Error: field name does not match the list field name");
        //                     return Some(Err(ParseError::MissingListContext {
        //                         path: path.clone(),
        //                     }));
        //                 }
        //
        //                 match field.data_type() {
        //                     DataType::List(item_type) => match (value, item_type.as_ref()) {
        //                         (Value::Boolean(_), DataType::Boolean)
        //                         | (Value::Integer(_), DataType::Integer)
        //                         | (Value::String(_), DataType::String) => {
        //                             let column_value = self.get_column_from_scalar_list(
        //                                 &path,
        //                                 &field,
        //                                 value,
        //                                 repetition_level,
        //                                 definition_level,
        //                             );
        //                             println!("Column-striped value: {:?}", column_value);
        //                             return Some(column_value);
        //                         }
        //                         (Value::Struct(props), DataType::Struct(fields)) => {
        //                             if props.is_empty() {
        //                                 let missing_paths =
        //                                     find_missing_paths(&path, props, fields, &self.paths);
        //                                 println!("Oops! This struct value is empty! Need to handle all missing paths here! {:#?}", missing_paths);
        //                             }
        //                             println!("Add a new frame to struct stack");
        //                             self.state
        //                                 .struct_stack
        //                                 .push(StructContext::new(fields.to_vec(), path.clone()));
        //                         }
        //                         _ => todo!("value type does not match data type"),
        //                     },
        //                     _ => unreachable!("expected a list item"),
        //                 }
        //             }
        //             _ => return Some(Err(ParseError::from(type_check_err))),
        //         },
        //     }
        // }
        //
        // // Handle missing fields at struct top-level
        // if let Some(missing_path) = self.state.missing_path_frames.next() {
        //     println!("Processing missing_path: {:?}", missing_path);
        //     // Required to correctly setup stack contexts
        //     self.state.handle_backtracking(missing_path.path());
        //     self.state.prev_path = PathVector::from(missing_path.path());
        //
        //     let data_type = missing_path.field().data_type();
        //     match data_type {
        //         DataType::Boolean | DataType::Integer | DataType::String => {
        //             Some(self.get_column_from_scalar(
        //                 &PathVector::from_slice(missing_path.path()),
        //                 &Value::create_null_or_empty(data_type),
        //             ))
        //         }
        //         DataType::List(inner) => match inner.as_ref() {
        //             DataType::Boolean | DataType::Integer | DataType::String => {
        //                 Some(self.get_column_from_scalar(
        //                     &PathVector::from_slice(missing_path.path()),
        //                     &Value::create_null_or_empty(inner.as_ref()),
        //                 ))
        //             }
        //             DataType::List(_) => {
        //                 unreachable!("not allowed")
        //             }
        //             DataType::Struct(_) => {
        //                 unreachable!("leaf field cannot have a struct as list item")
        //             }
        //         },
        //         DataType::Struct(_) => {
        //             unreachable!("leaf field cannot be a struct value")
        //         }
        //     }
        // } else {
        //     None
        // }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::schema::{
        bool, integer, optional_group, optional_integer, repeated_group, repeated_integer,
        required_group, string,
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

        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[0].value, Value::Integer(Some(20)));
        assert_eq!(parsed[1].value, Value::Integer(Some(40)));
        assert_eq!(parsed[2].value, Value::Integer(Some(60)));

        assert_eq!(parsed[0].definition_level, 2);
        assert_eq!(parsed[1].definition_level, 2);
        assert_eq!(parsed[2].definition_level, 2);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 1);
        assert_eq!(parsed[2].repetition_level, 1);
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

        assert_eq!(parsed[0].value, Value::String(Some(String::from("en-us"))));
        assert_eq!(parsed[1].value, Value::String(Some(String::from("en"))));
        assert_eq!(parsed[2].value, Value::String(None));
        assert_eq!(parsed[3].value, Value::String(Some(String::from("en-gb"))));

        assert_eq!(parsed[0].definition_level, 2);
        assert_eq!(parsed[1].definition_level, 2);
        assert_eq!(parsed[2].definition_level, 1);
        assert_eq!(parsed[3].definition_level, 2);

        assert_eq!(parsed[0].repetition_level, 0);
        assert_eq!(parsed[1].repetition_level, 2);
        assert_eq!(parsed[2].repetition_level, 1);
        assert_eq!(parsed[3].repetition_level, 1);
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
