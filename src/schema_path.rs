//! Internal representation of a path as a sequence of strings.

use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};

/// Provides a type-safe representation for paths in a nested value and
/// path specific methods.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct SchemaPath(Vec<String>);

impl Deref for SchemaPath {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SchemaPath {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<&[&str]> for SchemaPath {
    fn from(slice: &[&str]) -> Self {
        SchemaPath(slice.iter().map(|s| s.to_string()).collect())
    }
}

impl From<&[String]> for SchemaPath {
    fn from(slice: &[String]) -> Self {
        SchemaPath(slice.to_vec())
    }
}

impl From<Vec<String>> for SchemaPath {
    fn from(vec: Vec<String>) -> Self {
        SchemaPath(vec)
    }
}

impl Display for SchemaPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "<root>")
        } else {
            write!(f, "{}", self.join("."))
        }
    }
}

impl SchemaPath {
    /// Checks if path represents the root (is empty)
    pub fn is_root(&self) -> bool {
        self.is_empty()
    }

    /// Creates a new `SchemaPath` by appending a path component.
    pub fn append_name(&self, name: String) -> Self {
        SchemaPath(self.iter().cloned().chain(std::iter::once(name)).collect())
    }

    /// Returns the count of components(depth) in a path
    pub fn depth(&self) -> usize {
        self.len()
    }

    /// Creates a new `SchemaPath` containing the first `len` components.
    pub fn prefix(&self, len: usize) -> SchemaPath {
        SchemaPath(self.iter().take(len).cloned().collect())
    }
}
