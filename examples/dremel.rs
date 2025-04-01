use denester::parser::ValueParser;
use denester::schema::{repeated_group, string, SchemaBuilder};
use denester::value::ValueBuilder;

/// # Schema
/// message doc {
///     repeated group Name {
///         repeated group Language {
///             required string Code;
///         }
///     }
/// }
fn main() {
    let schema = SchemaBuilder::new(
        "doc",
        vec![repeated_group(
            "Name",
            vec![repeated_group("Language", vec![string("Code")])],
        )],
    )
    .build();

    let value = ValueBuilder::new()
        .repeated(
            "Name",
            vec![
                ValueBuilder::new() // 0
                    .repeated(
                        "Language",
                        vec![
                            ValueBuilder::new().field("Code", "en-us").build(),
                            ValueBuilder::new().field("Code", "en").build(),
                        ],
                    )
                    .build(),
                ValueBuilder::new().build(), // 1
                ValueBuilder::new() // 2
                    .repeated(
                        "Language",
                        vec![ValueBuilder::new().field("Code", "en-gb").build()],
                    )
                    .build(),
            ],
        )
        .build();

    let mut parser = ValueParser::new(&schema, value.iter_depth_first());
    while let Some(column) = parser.next() {
        println!("{:#?}", column);
    }
}
