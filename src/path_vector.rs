// TODO: Convert `PathVector` to newtype instead of a type alias
pub(crate) type PathVector = Vec<String>;
pub(crate) type PathVectorSlice<'a> = &'a [String];

pub(crate) trait PathVectorExt {
    fn format(&self) -> String;
    fn is_root(&self) -> bool;
    fn append_name(&self, name: String) -> PathVector;
    fn from_slice(slice: PathVectorSlice) -> Self;
    fn root() -> Self;
    fn depth(&self) -> usize;
    fn prefix(&self, len: usize) -> PathVector;
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

    fn depth(&self) -> usize {
        self.len()
    }

    fn prefix(&self, len: usize) -> PathVector {
        self.iter().take(len).cloned().collect()
    }
}
