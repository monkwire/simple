// use crate::parser::db_interface::write_table; 
use crate::db_interface::db_interface::{
    write_table,
    DataType,
    Column,
};

// Remove the nested module declaration
pub fn parse_create_table(raw_sql: String) {
    println!("parse_create_table func: {}", raw_sql);

    let columns = vec![Column{title: "temp title".to_string(), kind: DataType::STRING, required: true}];

    write_table(columns, "this is a title");
}