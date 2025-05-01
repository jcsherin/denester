//! Internal representation of a path as a sequence of strings.

use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};

/// Provides a type-safe representation for paths in a nested value and
/// path specific methods.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct PathVector(Vec<String>);

impl Deref for PathVector {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PathVector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<&[&str]> for PathVector {
    fn from(slice: &[&str]) -> Self {
        PathVector(slice.iter().map(|s| s.to_string()).collect())
    }
}

impl From<&[String]> for PathVector {
    fn from(slice: &[String]) -> Self {
        PathVector(slice.to_vec())
    }
}

impl From<Vec<String>> for PathVector {
    fn from(vec: Vec<String>) -> Self {
        PathVector(vec)
    }
}

impl Display for PathVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "<root>")
        } else {
            write!(f, "{}", self.join("."))
        }
    }
}

impl PathVector {
    /// Checks if path represents the root (is empty)
    pub fn is_root(&self) -> bool {
        self.is_empty()
    }

    /// Creates a new `PathVector` by appending a path component.
    pub fn append_name(&self, name: String) -> Self {
        PathVector(self.iter().cloned().chain(std::iter::once(name)).collect())
    }

    /// Returns the count of components(depth) in a path
    pub fn depth(&self) -> usize {
        self.len()
    }

    /// Creates a new `PathVector` containing the first `len` components.
    pub fn prefix(&self, len: usize) -> PathVector {
        PathVector(self.iter().take(len).cloned().collect())
    }
}
