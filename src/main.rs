use std::sync::Arc;
mod parser;
use parser::parse_create_table;
mod db_interface;

fn main() {
    println!("main func");
    // let PATH = Arc::new("../data.parquet");
    parse_create_table("this is a test".to_string());




}
