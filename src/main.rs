mod record;
use crate::record::{DataType, Field};

fn main() {
    let name = Field::new("name", DataType::String, false);

    println!("Field name: {}", name.name());
    println!("Field type: {:?}", name.data_type());
    println!("Is nullable: {}", name.is_nullable());
}
