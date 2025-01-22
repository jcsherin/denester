use crate::record::Field;

#[derive(Debug)]
pub struct FieldPath<'a> {
    field: &'a Field,
    path: Vec<String>,
}

impl<'a> FieldPath<'a> {
    pub fn new(field: &'a Field, path: Vec<String>) -> Self {
        Self { field, path }
    }

    pub fn field(&self) -> &'a Field {
        &self.field
    }

    pub fn path(&self) -> &[String] {
        &self.path
    }
}

pub struct FieldPathIterator<'a> {
    fields: Vec<std::slice::Iter<'a, Field>>,
    current_path: Vec<String>,
}

impl<'a> Iterator for FieldPathIterator<'a> {
    type Item = FieldPath<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::record::field_path::FieldPath;
    use crate::record::{DataType, Field};

    #[test]
    fn test_field_path() {
        let email = Field::new("email", DataType::String, true);
        let path = vec![
            String::from("customer"),
            String::from("address"),
            String::from("email"),
        ];

        let actual = FieldPath::new(&email, path);

        assert_eq!(actual.field(), &email);
        assert_eq!(actual.path()[0], String::from("customer"));
        assert_eq!(actual.path()[1], String::from("address"));
        assert_eq!(actual.path()[2], String::from("email"));
    }
}
