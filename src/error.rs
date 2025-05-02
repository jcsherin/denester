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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DenesterError::NullValueInRequiredField {
                field_name,
                type_label,
                path_str,
            } => {
                write!(
                    f,
                    "Expected datatype: '{}' value but found NULL instead, field: {} path: {}",
                    type_label, field_name, path_str
                )
            }
            DenesterError::MissingOneOrMoreRequiredValues {
                missing_field_names,
                path_str,
            } => {
                write!(
                    f,
                    "Struct value is missing ({}) field name at path: {}",
                    missing_field_names.join(","),
                    path_str
                )
            }
            DenesterError::InputValueMustBeAStruct => {
                write!(f, "Parser input is not a struct value.")
            }
            DenesterError::ListElementDoesNotMatchSchema {
                field_name,
                path_str,
                index,
            } => {
                write!(
                    f,
                    "List element at index: {} in field: {} at path: {}\
                        does not match the list element type defined in schema",
                    index, field_name, path_str
                )
            }
            DenesterError::ValueTypeDoesNotMatchSchema {
                field_name,
                path_str,
                expected_type_label,
            } => {
                write!(
                    f,
                    "Value datatype does not match expected type: {} for field: {} at path: {}",
                    expected_type_label, field_name, path_str
                )
            }
            DenesterError::StructContainsDuplicateProperty {
                dup_property_name,
                path_str,
            } => {
                write!(
                    f,
                    "Struct contains duplicate property: {} at path: {}",
                    dup_property_name, path_str
                )
            }
            DenesterError::StructContainsUndefinedProperty {
                undefined_property_name,
                path_str,
            } => {
                write!(
                    f,
                    "Struct contains undefined property: {} at path: {}",
                    undefined_property_name, path_str
                )
            }
        }
    }
}

impl std::error::Error for DenesterError {}
