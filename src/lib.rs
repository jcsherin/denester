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

pub mod common;
pub mod field;
pub mod field_path;
pub mod parser;
mod path_vector;
pub mod schema;
pub mod value;

pub use self::parser::ValueParser;
pub use self::schema::SchemaBuilder;
pub use self::value::ValueBuilder;
