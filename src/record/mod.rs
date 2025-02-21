mod field;
mod field_path;
mod parser;
mod schema;
mod value;

pub use field::{DataType, Field};
pub use schema::{Schema, SchemaBuilder};
use std::path::Path;
pub use value::Value;

type PathVector = Vec<String>;
type PathVectorSlice<'a> = &'a [String];

trait PathVectorExt {
    fn longest_common_prefix(&self, other: &PathVector) -> PathVector;
    fn format(&self) -> String;
    fn is_top_level(&self) -> bool;
    fn append_name(&self, name: String) -> PathVector;
    fn from_slice(slice: PathVectorSlice) -> Self;
}

impl PathVectorExt for PathVector {
    fn longest_common_prefix(&self, other: &PathVector) -> PathVector {
        self.iter()
            .zip(other.iter())
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| a.clone())
            .collect::<Vec<_>>()
    }

    fn format(&self) -> String {
        if self.is_empty() {
            ".top-level".to_string()
        } else {
            self.join(".")
        }
    }

    fn is_top_level(&self) -> bool {
        self.is_empty()
    }

    /// Creates a new PathVector by appending the input field name
    fn append_name(&self, name: String) -> PathVector {
        self.iter().cloned().chain(std::iter::once(name)).collect()
    }

    fn from_slice(slice: PathVectorSlice) -> Self {
        slice.to_vec()
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

#[cfg(test)]
mod tests {
    use crate::record::{PathVector, PathVectorExt};

    #[test]
    fn test_common_path_exists() {
        let path1: PathVector = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let path2: PathVector = vec!["a".to_string(), "b".to_string(), "d".to_string()];

        assert_eq!(
            path1.longest_common_prefix(&path2),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn test_common_path_does_not_exist() {
        let path1: PathVector = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let path2: PathVector = vec!["x".to_string(), "b".to_string(), "d".to_string()];

        assert_eq!(path1.longest_common_prefix(&path2), PathVector::default());
    }

    #[test]
    fn test_common_path_against_empty() {
        let path1: PathVector = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let path2 = PathVector::default();

        assert_eq!(path1.longest_common_prefix(&path2), PathVector::default());
        assert_eq!(path2.longest_common_prefix(&path1), PathVector::default());
    }

    #[test]
    fn test_common_path_backtracking() {
        // Path transition from a.b.c to a.x in depth-first traversal
        let path1: PathVector = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let path2: PathVector = vec!["a".to_string(), "x".to_string()];

        assert_eq!(path1.longest_common_prefix(&path2), vec!["a".to_string()]);
    }

    #[test]
    fn test_common_path_identical() {
        let path1 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(path1.longest_common_prefix(&path1), path1);
    }
}
