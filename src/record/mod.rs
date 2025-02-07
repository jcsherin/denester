mod field;
mod field_path;
mod parser;
mod schema;
mod value;

pub use field::{DataType, Field};
pub use schema::{Schema, SchemaBuilder};
pub use value::Value;

type PathVector = Vec<String>;
struct FieldLevel<'a> {
    iter: std::slice::Iter<'a, Field>,
    path: PathVector,
}

impl<'a> FieldLevel<'a> {
    fn new(iter: std::slice::Iter<'a, Field>, path: Vec<String>) -> Self {
        Self { iter, path }
    }
}
