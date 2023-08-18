use db_interface::write_table;
use crate::Arc;
use std::sync::Arc;

mod parser {
    pub fn parse_create_table(raw_sql: String) {
        columns = [Column::new(title=raw_sql, kind=String, required=True)];
        db_interface::write_table(columns, "this is a title");


    }
}