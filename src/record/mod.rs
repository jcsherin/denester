mod field;
mod field_path;
mod parser;
mod schema;
mod value;

pub use field::{DataType, Field};
pub use schema::{Schema, SchemaBuilder};
pub use value::Value;

// TODO: Convert `PathVector` to newtype instead of a type alias
type PathVector = Vec<String>;
type PathVectorSlice<'a> = &'a [String];

trait PathVectorExt {
    fn format(&self) -> String;
    fn is_root(&self) -> bool;
    fn append_name(&self, name: String) -> PathVector;
    fn from_slice(slice: PathVectorSlice) -> Self;
    fn root() -> Self;
}

impl PathVectorExt for PathVector {
    fn format(&self) -> String {
        if self.is_empty() {
            ".top-level".to_string()
        } else {
            self.join(".")
        }
    }

    fn is_root(&self) -> bool {
        self.is_empty()
    }

    /// Creates a new PathVector by appending the input field name
    fn append_name(&self, name: String) -> PathVector {
        self.iter().cloned().chain(std::iter::once(name)).collect()
    }

    fn from_slice(slice: PathVectorSlice) -> Self {
        slice.to_vec()
    }

    fn root() -> Self {
        Self::default()
    }
}

struct FieldLevel<'a> {
    iter: std::slice::Iter<'a, Field>,
    path: PathVector,
}

impl<'a> FieldLevel<'a> {
    fn new(iter: std::slice::Iter<'a, Field>, path: Vec<String>) -> Self {
        Self { iter, path }
    }
}
