//! Denester error types

use std::fmt::{Display, Formatter};

/// Result type for [`DenesterError`]
pub type Result<T, E = DenesterError> = std::result::Result<T, E>;

/// Error Type
#[derive(Debug)]
pub enum DenesterError {
    /// A required field contained a null value.
    NullValueInRequiredField {
        /// The field name which is required but contains null.
        field_name: String,
        /// A string label representing the data type of the required field.
        type_label: String,
        /// The dot-separated path indicating the location of the field.
        path_str: String,
    },

    /// One or more required fields were missing from a struct value.
    MissingOneOrMoreRequiredValues {
        /// All the required field names in the struct which are missing.
        missing_field_names: Vec<String>,
        /// The dot-separated path indicating the location of the field.
        path_str: String,
    },

    /// The top-level input value to parser was not a struct.
    InputValueMustBeAStruct,

    /// An element within a list did not match the list's defined element type
    /// in the schema.
    ListElementDoesNotMatchSchema {
        /// The field name of the list which contains this element.
        field_name: String,
        /// The dot-separated path indicating the location of the field.
        path_str: String,
        /// The list index indicating the location of the schema mismatch.
        index: usize,
    },

    /// The type of value did not match the expected field type defined in the
    /// schema.
    ValueTypeDoesNotMatchSchema {
        /// The field name which did not match the schema data type.
        field_name: String,
        /// The dot-separated path indicating the location of the field.
        path_str: String,
        /// A string label representing the expected data type for this field.
        expected_type_label: String,
    },

    /// A struct value contained a duplicate property name.
    StructContainsDuplicateProperty {
        /// The field name which has duplicate entries in the value.
        dup_property_name: String,
        /// The dot-separated path indicating the location of the field.
        path_str: String,
    },

    /// A property name found in the struct is not defined in the schema
    StructContainsUndefinedProperty {
        /// The field name which is undefined in the struct schema.
        undefined_property_name: String,
        /// The dot-separated path indicating the location of the field.
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
                    "Expected datatype: '{type_label}' value but found NULL instead, field: {field_name} path: {path_str}",
                )
            }
            DenesterError::MissingOneOrMoreRequiredValues {
                missing_field_names,
                path_str,
            } => {
                let missing_field_names = missing_field_names.join(", ");
                write!(
                    f,
                    "Struct value is missing ({missing_field_names}) field name at path: {path_str}",
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
                    "List element at index: {index} in field: {field_name} at path: {path_str}\
                        does not match the list element type defined in schema",
                )
            }
            DenesterError::ValueTypeDoesNotMatchSchema {
                field_name,
                path_str,
                expected_type_label,
            } => {
                write!(
                    f,
                    "Value datatype does not match expected type: {expected_type_label} for field: {field_name} at path: {path_str}",
                )
            }
            DenesterError::StructContainsDuplicateProperty {
                dup_property_name,
                path_str,
            } => {
                write!(
                    f,
                    "Struct contains duplicate property: {dup_property_name} at path: {path_str}",
                )
            }
            DenesterError::StructContainsUndefinedProperty {
                undefined_property_name,
                path_str,
            } => {
                write!(
                    f,
                    "Struct contains undefined property: {undefined_property_name} at path: {path_str}",
                )
            }
        }
    }
}

impl std::error::Error for DenesterError {}
