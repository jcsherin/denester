//! Implements the core logic for parsing nested values according to a schema
//! and producing a flattened representation suitable for columnar storage.

use crate::common::{DefinitionLevel, RepetitionDepth, RepetitionLevel};
use crate::field::{DataType, Field};
use crate::field_path::{FieldPath, PathMetadata, PathMetadataIterator};
use crate::parser::ParseError::{RequiredFieldIsNull, RequiredFieldsAreMissing};
use crate::path_vector::{PathVector, PathVectorExt, PathVectorSlice};
use crate::schema::Schema;
use crate::value::{DepthFirstValueIterator, TypeCheckError, Value};
use std::collections::{HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::iter::Peekable;
use std::ops::Deref;

/// TODO: Add docs after reviewing error handling
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
    FieldNameLookupInRoot,
    FieldNameLookupMissingContext {
        field_name: String,
        path: Vec<String>,
    },
    FieldNameLookupFailed {
        field_name: String,
        path: Vec<String>,
        ctx: StructContext,
    },
    RootIsNotStruct,
    MissingFieldsContext,
    MissingLevelContext {
        path: PathVector,
    },
    RequiredFieldsAreMissing {
        missing: Vec<String>,
        path: PathVector,
    },
    ListTypeMismatch {
        path: Vec<String>,
    },
}

impl From<TypeCheckError> for ParseError<'_> {
    fn from(err: TypeCheckError) -> Self {
        match err {
            TypeCheckError::DataTypeMismatch { .. } => {
                todo!()
            }
            TypeCheckError::RequiredFieldIsNull { path, field } => RequiredFieldIsNull {
                field_path: FieldPath::new(field, path.to_vec()),
            },
            TypeCheckError::RequiredFieldsAreMissing { missing, path } => {
                RequiredFieldsAreMissing { missing, path }
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

impl Display for ParseError<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RequiredFieldIsNull { field_path } => {
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
            ParseError::FieldNameLookupInRoot => {
                write!(f, "Cannot lookup field by name at root")
            }
            ParseError::FieldNameLookupMissingContext { field_name, path } => {
                write!(
                    f,
                    "Context missing for lookup of field name: {} in path: {:#?}",
                    field_name, path
                )
            }
            ParseError::FieldNameLookupFailed {
                field_name,
                path,
                ctx,
            } => {
                write!(
                    f,
                    "Field name: {} in path: {:#?} not found in context: {:#?}",
                    field_name, path, ctx
                )
            }
            ParseError::RootIsNotStruct => {
                write!(f, "Root is not a struct type value")
            }
            ParseError::MissingFieldsContext => {
                write!(f, "Root field definitions are missing in context")
            }
            ParseError::RequiredFieldsAreMissing { missing, path } => {
                write!(
                    f,
                    "Required fields missing: {} in path: {}",
                    missing.join(", "),
                    path.format()
                )
            }
            ParseError::ListTypeMismatch { path } => {
                write!(
                    f,
                    "Type checking failed for list element: {}",
                    path.format()
                )
            }
        }
    }
}

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

#[derive(Debug)]
struct ListContext {
    field_name: String,
    current_index: usize,
    depth: usize,
}

impl ListContext {
    fn new(field_name: String, depth: usize) -> Self {
        Self {
            field_name,
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

    pub fn position(&self) -> usize {
        self.current_index
    }

    pub fn advance(&mut self) {
        self.current_index += 1;
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

    /// Peek at next item without consuming it
    fn peek(&self) -> Option<&T> {
        self.stack
            .iter()
            .rev()
            .find(|frame| !frame.is_empty())
            .and_then(|frame| frame.front())
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
        while self.stack.last().is_some_and(VecDeque::is_empty) {
            self.stack.pop();
        }

        // Get an item from first non-empty deque
        let item = self.stack.last_mut()?.pop_front();

        // Pop the deque if it is now empty
        if self.stack.last().is_some_and(VecDeque::is_empty) {
            self.stack.pop();
        }

        item
    }
}

#[derive(Debug, Default, Clone)]
pub struct StructContext {
    fields: Vec<Field>,
    path: PathVector,
}

impl StructContext {
    fn new(fields: Vec<Field>, path: PathVector) -> Self {
        Self { fields, path }
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
    NoMoreWork,
}

#[derive(Debug, Default)]
struct ValueParserState {
    struct_context_stack: Vec<StructContext>,
    list_stack: Vec<ListContext>,
    prev_path: PathVector,
    computed_levels: Vec<LevelContext>,
    missing_paths_buffer: DequeStack<PathMetadata>,
}

impl ValueParserState {
    fn new(schema: &Schema) -> Self {
        if schema.is_empty() {
            Self::default()
        } else {
            Self {
                struct_context_stack: vec![StructContext::new(
                    schema.fields().to_vec(),
                    PathVector::root(),
                )],
                list_stack: vec![],
                prev_path: PathVector::root(),
                computed_levels: vec![LevelContext::default()],
                missing_paths_buffer: DequeStack::new(),
            }
        }
    }

    fn current_struct_context(&self) -> Option<&StructContext> {
        self.struct_context_stack.last()
    }

    /// Manages state during tree traversal level transitions.
    ///
    /// When a level transition is detected, this method prunes the internal stacks. A level
    /// transition occurs when the traversal path changes direction from going downwards to either
    /// sideways or upwards in the value tree.
    fn transition_to(&mut self, curr_path: PathVectorSlice) {
        self.prune_stacks(curr_path);

        // Saves the current path for the next traversal step. Each call to this method requires the
        // previous path to properly identify level transitions.
        self.prev_path = PathVector::from(curr_path);
    }

    /// Removes stack frames when the traversal backtracks to ancestors or siblings.
    ///
    /// This method is not meant to be used directly. Instead, see [`transition_to`].
    ///
    /// This operation ensures consistency across multiple internal tracking states:
    ///     - schema validation stack: field definitions for the node we are currently visiting
    ///     - levels stack: computed definition, repetition and depth values
    ///     - list iterator stack: tracks position of repeated item during traversal
    ///
    /// After pruning, the top of all the stacks will align with the currently visited node.
    fn prune_stacks(&mut self, curr_path: PathVectorSlice) {
        // Skip pruning if we are going deeper into the tree (not backtracking). A level transition
        // occurs only when moving to either siblings or ancestors.
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

        while self.struct_context_stack.len() > 1 {
            let top = self.struct_context_stack.last().unwrap();

            if top.path().is_root()
                || (top.path().len() < curr_path.len() && curr_path.starts_with(top.path()))
            {
                break;
            }

            self.struct_context_stack.pop().unwrap();
        }

        // TODO: add `depth` method to PathVector
        let curr_depth = curr_path.len();
        while !self.list_stack.is_empty() && self.list_stack.last().unwrap().depth() > curr_depth {
            self.list_stack.pop().unwrap();
        }
    }
}

/// Parses a nested [`Value`] according to a [`Schema`], yielding flattened
/// column values.
///
/// This struct implements the [`Iterator`] trait, where each item represents
/// a single column value extracted from the nested structure, along with its
/// computed definition and repetition levels described in the Dremel paper.
///
/// If a path present in the schema is missing in the value, or if it
/// terminates early then a null value is produced with the definition and
/// repetition levels.
#[derive(Debug)]
pub struct ValueParser<'a> {
    /// Precomputed paths and max definition and repetition levels computed
    /// from [`Schema`].
    paths: Vec<PathMetadata>,
    /// A depth-first iterator over the input nested [`Value`].
    value_iter: Peekable<DepthFirstValueIterator<'a>>,
    /// The internal state machine
    state: ValueParserState,
    /// A queue for decoupling depth-first value traversal from its processing
    /// to simplify generating null values for missing paths.
    work_queue: VecDeque<WorkItem<'a>>,
}

impl<'a> ValueParser<'a> {
    /// Creates a new `ValueParser`.
    ///
    /// # Parameters
    /// * `schema` - Reference schema for validating the input nested value.
    /// * `value_iter` - A depth-first value iterator over the input nested value.
    pub fn new(schema: &'a Schema, value_iter: DepthFirstValueIterator<'a>) -> Self {
        let paths = PathMetadataIterator::new(schema).collect::<Vec<_>>();
        let state = ValueParserState::new(schema);

        let value_iter = value_iter.peekable();
        let work_queue = VecDeque::new();

        Self {
            paths,
            value_iter,
            state,
            work_queue,
        }
    }

    /// Lookup a field by resolving path using the field definitions context
    fn get_field_by_path(&self, path: &PathVector) -> Result<&Field, ParseError<'a>> {
        if path.is_root() {
            return Err(ParseError::FieldNameLookupInRoot);
        }

        let field_name = path.last().unwrap();

        self.state
            .current_struct_context()
            .ok_or_else(|| ParseError::FieldNameLookupMissingContext {
                field_name: field_name.to_string(),
                path: path.clone(),
            })
            .and_then(|ctx| {
                ctx.find_field(field_name)
                    .ok_or_else(|| ParseError::FieldNameLookupFailed {
                        field_name: field_name.to_string(),
                        path: path.clone(),
                        ctx: ctx.clone(),
                    })
            })
    }

    /// Computes level metadata from field definition and updates the computed levels stack
    ///
    /// This method is called when visiting internal value nodes. It updates the level stack which
    /// tracks the definition, repetition levels from root to leaf node. At the leaf node the most
    /// recent stack entry is used to construct a column value with the correct definition and
    /// repetition levels.
    fn push_derived_level_context(
        &mut self,
        field: &Field,
        path: &PathVector,
    ) -> Result<(), ParseError<'a>> {
        let parent_ctx = self
            .state
            .computed_levels
            .last()
            .ok_or_else(|| ParseError::MissingLevelContext { path: path.clone() })?;

        let new_ctx = parent_ctx.with_field(field, path);
        self.state.computed_levels.push(new_ctx);

        Ok(())
    }

    /// Adds a frame to stack for tracking a list element position during depth-first traversal
    ///
    /// Knowing the index of a list element during traversal is necessary to correctly compute the
    /// repetition levels for repeated items.
    ///
    /// In a nested value there maybe multiple nested list values in a path. The stack makes it
    /// trivial to track the list element index during depth-first traversal at any nesting level.
    fn push_list_iterator_context(&mut self, field: &Field, path: &PathVector) {
        self.state
            .list_stack
            .push(ListContext::new(field.name().to_string(), path.len()));
    }

    /// Adds a frame to stack with field definitions for type-checking children
    ///
    /// A struct value is sequence of key-value pairs. The struct datatype contains the field
    /// definitions required to type-check the struct value. The key in the struct value is used
    /// to lookup the corresponding field definition during value traversal.
    ///
    /// This context also allows us to identify which optional/repeated fields are missing in the
    /// value and buffer them for generating NULL column values after the present fields are
    /// processed during traversal.
    fn push_fields_context(&mut self, field: &Field, path: &PathVector) {
        let fields = self.get_struct_fields(field).to_vec();

        self.state
            .struct_context_stack
            .push(StructContext::new(fields, path.clone()));
    }

    /// Identifies missing optional/repeated fields for NULL output generation
    ///
    /// When optional/repeated fields are missing in a value during column-striping we need to
    /// generate a NULL value output.
    ///
    /// This works alongside the depth-first traversal to ensure proper interleaving of present
    /// values and NULL output for missing values.
    fn buffer_missing_paths(
        &mut self,
        fields: &[Field],
        path_prefix: &PathVector,
        props: &[(String, Value)],
    ) {
        self.state
            .missing_paths_buffer
            .push_frame(find_missing_paths(path_prefix, props, fields, &self.paths));
    }

    /// Extracts field definitions from a struct datatype or from a list of structs.
    fn get_struct_fields<'b>(&self, field: &'b Field) -> &'b [Field] {
        match field.data_type() {
            DataType::Struct(fields) => fields,
            DataType::List(element_type) => match element_type.as_ref() {
                DataType::Boolean | DataType::Integer | DataType::String => {
                    panic!("expected list element type to be a struct datatype")
                }
                DataType::List(_) => {
                    unreachable!("nested list is illegal")
                }
                DataType::Struct(fields) => fields,
            },
            DataType::Boolean | DataType::Integer | DataType::String => {
                panic!("expected struct datatype with field definitions")
            }
        }
    }

    /// Creates NULL column values for fields present in schema but missing from input data.
    fn create_null_column_for_missing_path(
        &mut self,
        missing_path: &PathMetadata,
    ) -> StripedColumnResult<'a> {
        self.state.transition_to(missing_path.path());

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

    /// Checks if supplied field matches the list iterator context
    fn is_matching_list_iterator_context(&self, field: &Field) -> bool {
        match self.state.list_stack.last() {
            None => false,
            Some(ctx) => ctx.field_name() == field.name(),
        }
    }

    /// Computes repetition, definition level from state for a list element
    ///
    /// The repetition level of a list element is dependent on its index. To be able to reassemble
    /// the column-striped record back, the first element has a different derivation of repetition
    /// level than other items.
    ///
    /// The definition level of a list item is independent of its index.
    fn get_level_context(&self, path: &PathVector) -> Result<LevelContext, ParseError<'a>> {
        let list_context = match self.state.list_stack.last() {
            None => return Err(ParseError::MissingListContext { path: path.clone() }),
            Some(ctx) => ctx,
        };

        let level_context = match self.state.computed_levels.last() {
            None => return Err(ParseError::MissingLevelContext { path: path.clone() }),
            Some(ctx) => ctx,
        };

        if list_context.position() > 0 {
            Ok(LevelContext {
                definition_level: level_context.definition_level,
                repetition_depth: level_context.repetition_depth,
                repetition_level: level_context.repetition_depth,
                path: path.clone(),
            })
        } else {
            Ok(LevelContext {
                definition_level: level_context.definition_level,
                repetition_depth: level_context.repetition_depth,
                repetition_level: level_context.repetition_level,
                path: path.clone(),
            })
        }
    }

    /// Advances the list iterator position by one
    fn advance_list_iterator(&mut self, path: &PathVector) -> Result<(), ParseError<'a>> {
        let ctx = match self.state.list_stack.last_mut() {
            None => return Err(ParseError::MissingListContext { path: path.clone() }),
            Some(ctx) => ctx,
        };

        ctx.advance();

        Ok(())
    }
}

/// Represents a single flattened column value produced by the [`ValueParser`].
///
/// Contains the extracted value along with its computed definition and
/// repetition level.
#[derive(Debug, PartialEq, Clone)]
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

    /// Returns a reference to the [`Value`] extracted from the nested value.
    ///
    /// This maybe a primitive value or a null value.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Returns the computed definition level.
    pub fn definition_level(&self) -> DefinitionLevel {
        self.definition_level
    }

    /// Returns the computed repetition level.
    pub fn repetition_level(&self) -> RepetitionLevel {
        self.repetition_level
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
    /// This method collects all the optional/repeated fields which are missing in the struct value.
    /// But it assumes that type-checking has been performed, which guarantees that for non-empty
    /// struct values, all required fields are present. And for an empty struct value, the struct
    /// definition has no required fields or type-checking would have failed because a required
    /// field is missing.
    fn with_struct(props: &[(String, Value)], fields: &[Field]) -> Self {
        let field_names = if !props.is_empty() {
            let present_fields = props
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<HashSet<_>>();

            fields
                .iter()
                .filter(|f| !present_fields.contains(f.name()))
                .map(|f| FieldName::from(f.name().to_string()))
                .collect()
        } else {
            fields
                .iter()
                .map(|f| FieldName::from(f.name().to_string()))
                .collect()
        };

        Self(field_names)
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
    props: &[(String, Value)],
    fields: &[Field],
    paths: &[PathMetadata],
) -> Vec<PathMetadata> {
    MissingFields::with_struct(props, fields)
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

    /// A column value is returned by traversing from root to leaf in depth-first order. For partial
    /// or missing paths, a NULL column value is returned. So after processing all the present
    /// properties of a struct, the depth-first traversal is suspended. Then the missing paths of
    /// the struct are processed in the order of field definitions in the schema. After that normal
    /// depth-first value traversal continues.
    ///
    /// There is a tight coupling between depth-first value traversal and computing column values.
    /// A column value is computed only after visiting a leaf node, or when processing the buffer
    /// for missing paths. This implementation uses a work queue to maintain the order of column
    /// value outputs, and makes processing of column values independent of traversal logic.
    ///
    /// Process work queue entries until it becomes empty. If the work queue is empty get the next
    /// value from the depth-first value iterator. If the traversal transitioned to either a sibling
    /// node or an ancestor node, then add a single frame of buffered missing paths to the work
    /// queue followed by the next value to the queue. So missing paths are processed after all the
    /// present properties. If the value iterator is exhausted, then add buffered missing paths
    /// to the work queue until the stack is empty. Finally, add a sentinel to signal end of value
    /// to the work queue.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.work_queue.is_empty() {
                let work_item = self.work_queue.pop_front().unwrap();
                match work_item {
                    WorkItem::Value(value, path) if path.is_root() => {
                        let props = match value {
                            Value::Struct(props) => props,
                            Value::Boolean(_)
                            | Value::Integer(_)
                            | Value::String(_)
                            | Value::List(_) => return Some(Err(ParseError::RootIsNotStruct)),
                        };
                        let fields = match &self.state.current_struct_context() {
                            None => return Some(Err(ParseError::MissingFieldsContext)),
                            Some(ctx) => &ctx.fields,
                        };

                        if let Err(err) = Value::type_check_struct_shallow(&path, props, fields) {
                            return Some(Err(err.into()));
                        }

                        self.buffer_missing_paths(&fields.to_vec(), &path, props);
                    }
                    WorkItem::Value(value, path) => {
                        self.state.transition_to(&path);

                        let field = match self.get_field_by_path(&path) {
                            Ok(field) => field.clone(),
                            Err(err) => return Some(Err(err)),
                        };
                        match value.type_check_shallow(&field, &path) {
                            Ok(()) => {
                                if let Err(err) = self.push_derived_level_context(&field, &path) {
                                    return Some(Err(err));
                                }

                                match value {
                                    Value::Boolean(_) | Value::Integer(_) | Value::String(_) => {
                                        return Some(self.get_column_from_scalar(&path, value))
                                    }
                                    Value::List(items) if items.is_empty() => {
                                        match field.data_type() {
                                            DataType::Boolean
                                            | DataType::Integer
                                            | DataType::String
                                            | DataType::Struct(_) => {
                                                unreachable!("Found Value::List but field's data type is not DataType::List")
                                            }
                                            DataType::List(item_data_type) => {
                                                match item_data_type.as_ref() {
                                                    DataType::Boolean
                                                    | DataType::Integer
                                                    | DataType::String => {
                                                        return Some(self.get_column_from_scalar(
                                                            &path,
                                                            &Value::create_null_or_empty(
                                                                item_data_type.as_ref(),
                                                            ),
                                                        ))
                                                    }
                                                    DataType::List(_) => {
                                                        unreachable!("nested list type is illegal")
                                                    }
                                                    DataType::Struct(fields) => {
                                                        self.buffer_missing_paths(
                                                            fields,
                                                            &path,
                                                            &[],
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Value::List(_) => {
                                        self.push_list_iterator_context(&field, &path);
                                    }
                                    Value::Struct(props) => {
                                        self.push_fields_context(&field, &path);

                                        let fields = self.get_struct_fields(&field);
                                        self.buffer_missing_paths(fields, &path, props);
                                    }
                                }
                            }
                            Err(type_check_error) => {
                                if !matches!(field.data_type(), DataType::List(_)) {
                                    return Some(Err(ParseError::from(type_check_error)));
                                }

                                if !self.is_matching_list_iterator_context(&field) {
                                    return Some(Err(ParseError::MissingListContext {
                                        path: path.clone(),
                                    }));
                                }

                                // Shallow type-checking intentionally skips list element type
                                // verification to keep the process simple.
                                //
                                // The list element failed earlier shallow type-checking because
                                // it is scalar value or struct value. But the field data type is
                                // `DataType::List(_)`. To be certain that the list element type
                                // matches, we need to compare the inner element type defined in
                                // `DataType::List(element_type)`.
                                //
                                // If verification fails we return a parse error. Otherwise, we
                                // proceed with column-striping the list element.
                                match field.data_type() {
                                    DataType::Boolean
                                    | DataType::Integer
                                    | DataType::String
                                    | DataType::Struct(_) => {
                                        unreachable!("expected a list data type")
                                    }
                                    DataType::List(element_type) => {
                                        match (element_type.as_ref(), value) {
                                            (DataType::Boolean, Value::Boolean(_))
                                            | (DataType::Integer, Value::Integer(_))
                                            | (DataType::String, Value::String(_)) => {
                                                // The index position is crucial to computing the repetition
                                                // level of a list element.
                                                //
                                                // We must therefore advance the iterator after getting the
                                                // level info, but before processing the element. This ensures
                                                // that the next element will find the iterator in the right
                                                // state when retrieving level info.
                                                let ctx = match self.get_level_context(&path) {
                                                    Ok(ctx) => {
                                                        match self.advance_list_iterator(&path) {
                                                            Ok(_) => ctx,
                                                            Err(err) => return Some(Err(err)),
                                                        }
                                                    }
                                                    Err(err) => return Some(Err(err)),
                                                };

                                                return Some(self.get_column_from_scalar_list(
                                                    &path,
                                                    &field,
                                                    value,
                                                    ctx.repetition_level,
                                                    ctx.definition_level,
                                                ));
                                            }
                                            (DataType::Struct(fields), Value::Struct(props)) => {
                                                // Struct values are handled differently from scalar list elements. The
                                                // struct value does not terminate at this level. They may contain other
                                                // nested values. The index of this struct value affects the repetition
                                                // level. These computed values must be propagated down through the entire
                                                // struct subtree.

                                                match self.get_level_context(&path) {
                                                    Err(err) => return Some(Err(err)),
                                                    Ok(ctx) => {
                                                        self.state.computed_levels.push(ctx);
                                                        if let Err(err) =
                                                            self.advance_list_iterator(&path)
                                                        {
                                                            return Some(Err(err));
                                                        }
                                                    }
                                                }

                                                self.push_fields_context(&field, &path);

                                                // If props is empty, add the missing paths directly to the work queue
                                                // bypassing the buffer.
                                                //
                                                // The work queue will be empty after processing this value because we
                                                // only add values incrementally. This gives us the opportunity to insert
                                                // these high-priority tasks at the front of the queue to be processed next.
                                                //
                                                // When a struct is empty, it means the path terminates at this point while
                                                // the schema defines a subtree that should exist. We need to generate NULL
                                                // column values for all the missing paths before processing siblings or
                                                // ancestors in the value tree.
                                                if props.is_empty() {
                                                    let missing_paths = find_missing_paths(
                                                        &path,
                                                        props,
                                                        fields,
                                                        &self.paths,
                                                    )
                                                    .into_iter()
                                                    .map(WorkItem::MissingValue);
                                                    self.work_queue.extend(missing_paths);
                                                } else {
                                                    // For non-empty structs, at least one path is present, so we buffer the
                                                    // missing paths. These will be added to the work queue when a backtracking
                                                    // path transition occurs during value traversal.
                                                    self.buffer_missing_paths(fields, &path, props);
                                                }
                                            }
                                            _ => {
                                                return Some(Err(ParseError::ListTypeMismatch {
                                                    path: path.clone(),
                                                }))
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    WorkItem::MissingValue(missing) => {
                        return Some(self.create_null_column_for_missing_path(&missing))
                    }
                    WorkItem::NoMoreWork => return None,
                }
            }

            if let Some((value, path)) = self.value_iter.next() {
                // Checks if path transitions to a sibling or an ancestor, and not to a child.
                //
                // The following path transitions are downwards:
                //     * Root to child (prev: ., curr: "a")
                //     * Downward to child (prev: "a.b", curr: "a.b.c")
                //
                // The following path transitions are either horizontal or upwards:
                //     * Sibling transitions (prev: "a.b", curr: "a.c")
                //     * Ancestor transitions (prev: "a.b.c", curr: "a")
                //     * Branch changes (prev: "a.b", curr: "c.d")
                //
                // There are paths which are defined in the schema but missing in the value. These
                // missing paths are buffered so that a NULL value can be output for each missing
                // path later.
                //
                // When transitioning to either a sibling (horizontal) or to an ancestor (upwards)
                // the missing paths which share a prefix with the previous path are to be
                // processed before the next value.
                //
                // If the previous path is "a.b" and the next value is in the path "c.d" and the
                // following missing paths are buffered: "a.b.x.y", "a.b.z". This ensures that
                // the order in which they are added to work queue is: "a.b.x.y", "a.b.z", "c.d".
                //
                // Missing paths are not handled in the following cases:
                // When path depth is equal to previous path then is a sibling transition.
                // When path depth is greater than previous path then it is descending the tree.
                //
                // Missing paths are processed when backtracking to an ancestor. At that point we
                // need to queue all the missing paths originating from the previous branch that
                // are pending.
                //
                // Example 1: `a.b.c` -> `a.x`.
                // The depth of current path is 2. So the prefix of depth 2 of the previous path is
                // `a.b`. Now examine missing paths which belong to the `a.b` branch. They need to
                // be processed before we process `a.x`.
                //
                // Example 2: `a.b.c` -> `x`
                // The depth of current path is 1. So the prefix of depth 1 of the previous path is
                // `a`. Now examine missing paths which belong to the `a` branch. They need to be
                // processed before we process `x`.
                //
                // Example 3: `a` -> `x` (`b`, `c` are missing)
                // This is a sibling transition, so we have to wait until all fields present in the
                // value whose parent is root to be processed. Only then do we process the missing
                // paths `b` and `c`.
                //
                // Example 4: `a` -> `a.b`
                // Here as there is no backtracking transition, we do not have to process any
                // missing paths.
                if path.depth() < self.state.prev_path.depth() {
                    let prefix_path = self.state.prev_path.prefix(path.depth());
                    let mut missing_paths = vec![];

                    while let Some(missing) = self.state.missing_paths_buffer.peek() {
                        // Stop because there are no more missing paths which shares the same prefix
                        // as the branch from which transitioned.
                        if !missing.path().starts_with(&prefix_path) {
                            break;
                        }

                        let item = self.state.missing_paths_buffer.next().unwrap();
                        missing_paths.push(WorkItem::MissingValue(item));
                    }

                    self.work_queue.extend(missing_paths);
                }

                self.work_queue.push_back(WorkItem::Value(value, path));
            } else {
                // Once the value iterator is exhausted all remaining missing paths which were
                // buffered earlier is added to the work queue for processing.
                if !self.state.missing_paths_buffer.is_empty() {
                    let remaining = std::mem::take(&mut self.state.missing_paths_buffer);
                    self.work_queue
                        .extend(remaining.map(WorkItem::MissingValue));
                }

                // Sentinel to signal the end. The value iterator is exhausted. All remaining
                // missing paths have been processed.
                self.work_queue.push_back(WorkItem::NoMoreWork);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{bool, integer, optional_integer, repeated_integer, string, SchemaBuilder};
    use crate::value::ValueBuilder;

    #[test]
    fn test_optional_field_contains_null() {
        let schema = SchemaBuilder::new("optional_field")
            .field(optional_integer("x"))
            .build();
        let value = ValueBuilder::default().integer("x", None).build();
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
        let schema = SchemaBuilder::new("optional_field")
            .field(optional_integer("x"))
            .build();
        let value = ValueBuilder::default().build();
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
        let schema = SchemaBuilder::new("optional_field")
            .field(optional_integer("x"))
            .build();
        let value = ValueBuilder::default().integer("x", Some(10)).build();
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
        let schema = SchemaBuilder::new("required_field")
            .field(integer("x"))
            .build();
        let value = ValueBuilder::default().field("x", 10).build();
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
        let schema = SchemaBuilder::new("required_field")
            .field(integer("x"))
            .build();
        let value = ValueBuilder::default().build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        assert!(matches!(parser.next().unwrap(),
                Err(RequiredFieldsAreMissing { missing, path })
                if path.is_root() && missing.len() == 1 && missing[0] == *"x"));
        assert!(parser.next().is_none());
    }

    #[test]
    fn test_required_field_contains_null() {
        let schema = SchemaBuilder::new("required_field")
            .field(integer("x"))
            .build();
        let value = ValueBuilder::default()
            .field("x", Value::Integer(None))
            .build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        assert!(matches!(parser.next().unwrap(),
            Err(RequiredFieldIsNull { field_path })
            if field_path.path() == ["x"] && field_path.field() == &integer("x")
        ));
        // TODO: add a test which contains other fields after the required field
        assert!(parser.next().is_none());
    }

    #[test]
    fn test_repeated_scalar_field_is_empty() {
        let schema = SchemaBuilder::new("repeated_field")
            .field(repeated_integer("xs"))
            .build();
        let value = ValueBuilder::default()
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
        let schema = SchemaBuilder::new("repeated_field")
            .field(repeated_integer("xs"))
            .build();
        let value = ValueBuilder::default()
            .repeated("xs", vec![1, 2, 3])
            .build();
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
        let schema = SchemaBuilder::new("repeated_field")
            .field(repeated_integer("xs"))
            .build();
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
        let schema = SchemaBuilder::new("repeated_field")
            .field(repeated_integer("xs"))
            .build();
        let value = ValueBuilder::default().build();
        let mut parser = ValueParser::new(&schema, value.iter_depth_first());

        let item = parser.next().unwrap();
        assert_eq!(item.as_ref().unwrap().value, Value::Integer(None));
        assert_eq!(item.as_ref().unwrap().definition_level, 0);
        assert_eq!(item.as_ref().unwrap().repetition_level, 0);

        assert!(parser.next().is_none());
    }

    #[test]
    fn test_simple_struct() {
        let schema = SchemaBuilder::new("user")
            .field(string("name"))
            .field(integer("id"))
            .field(bool("enrolled"))
            .field(repeated_integer("groups"))
            .build();

        let value = ValueBuilder::default()
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
    fn test_top_level_missing_fields() {
        // message doc {
        //   required int a,
        //   optional int b,
        //   optional int c, }
        let schema = SchemaBuilder::new("doc")
            .field(integer("a"))
            .field(optional_integer("b"))
            .field(optional_integer("c"))
            .build();

        // { a: 1, c: 3 }
        let value = ValueBuilder::default().field("a", 1).field("c", 3).build();

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
}
