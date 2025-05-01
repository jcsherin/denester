//! A library which implements column shredding of nested data structures by
//! encoding definition and repetition levels for each value. The definition
//! and repetition levels, preserves the structural hierarchy of the encoded
//! nested data structure. So it is possible to reconstruct the nested data
//! structure in its original form back from the encoding.
//!
//! # Design
//! The technique for column shredding is described in the paper:
//! [Dremel: Interactive Analysis of Web-Scale Datasets](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf).

#![warn(missing_debug_implementations)]
// #![warn(missing_docs)]

mod error;
pub mod field;
mod field_path;
pub mod parser;
pub mod schema;
mod schema_path;
pub mod value;

pub use self::parser::ValueParser;
pub use self::schema::SchemaBuilder;
pub use self::value::ValueBuilder;

/// Represents the path to a value within a nested structure.
pub type ValuePath = Vec<String>;
/// Represents the computed definition level of a flattened value
pub type DefinitionLevel = u8;
/// Represents the computed repetition level of a flattened value
pub type RepetitionLevel = u8;
/// Represents the distinct repeated fields leading to a flattened value
pub(crate) type RepetitionDepth = u8;
