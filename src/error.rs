//! Denester error types

use std::fmt::{Display, Formatter};

/// Result type for [`DenesterError`]
pub type Result<T, E = DenesterError> = std::result::Result<T, E>;

/// Error Type
#[derive(Debug)]
pub enum DenesterError {
    /// A required field contained a null value.
    NullValueInRequiredField {
        field_name: String,
        type_label: String,
        path_str: String,
    },

    /// One or more required fields were missing from a struct value.
    MissingOneOrMoreRequiredValues {
        missing_field_names: Vec<String>,
        path_str: String,
    },

    /// The top-level input value to parser was not a struct.
    InputValueMustBeAStruct,

    /// An element within a list did not match the list's defined element type
    /// in the schema.
    ListElementDoesNotMatchSchema {
        field_name: String,
        path_str: String,
        index: usize,
    },

    /// The type of value did not match the expected field type defined in the
    /// schema.
    ValueTypeDoesNotMatchSchema {
        field_name: String,
        path_str: String,
        expected_type_label: String,
    },

    /// A struct value contained a duplicate property name.
    StructContainsDuplicateProperty {
        dup_property_name: String,
        path_str: String,
    },

    /// A property name found in the struct is not defined in the schema
    StructContainsUndefinedProperty {
        undefined_property_name: String,
        path_str: String,
    },
}

impl Display for DenesterError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for DenesterError {}
