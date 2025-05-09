use denester::schema::{repeated_group, string};
use denester::{SchemaBuilder, ValueBuilder, ValueParser};

/// # Schema
/// message doc {
///     repeated group Name {
///         repeated group Language {
///             required string Code;
///         }
///     }
/// }
fn main() {
    let schema = SchemaBuilder::new("doc")
        .field(repeated_group(
            "Name",
            vec![repeated_group("Language", vec![string("Code")])],
        ))
        .build();

    let value = ValueBuilder::default()
        .repeated(
            "Name",
            vec![
                ValueBuilder::default() // 0
                    .repeated(
                        "Language",
                        vec![
                            ValueBuilder::default().field("Code", "en-us").build(),
                            ValueBuilder::default().field("Code", "en").build(),
                        ],
                    )
                    .build(),
                ValueBuilder::default().build(), // 1
                ValueBuilder::default() // 2
                    .repeated(
                        "Language",
                        vec![ValueBuilder::default().field("Code", "en-gb").build()],
                    )
                    .build(),
            ],
        )
        .build();

    let parser = ValueParser::new(&schema, value.iter_depth_first());
    for column in parser {
        println!("{:#?}", column);
    }
}
