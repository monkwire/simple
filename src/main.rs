use std::sync::Arc;
mod parser;

fn main() {
    let PATH = Arc::new("../data.parquet");
    parser::parse_create_table("this is a test");




}
