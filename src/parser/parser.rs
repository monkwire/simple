// use arrow::record_batch;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlparser::ast::{self, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

// #[derive(Debug)]
// pub enum TableValue {
//     StringValue(String),
//     Int32Value(i32),
//     FloatValue(f32),
//     BoolValue(bool),
// }
// Parse function returns a vec of the results of all SQL Statements. All successful statement
// results return tables.
pub fn parse(sql: &str, path: &str) {
    // Separate SQL statements on ';'
    let statements = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    // Create results table
    // let mut tables: Vec<HashMap<String, dyn arrow_array::Array>> = vec![];
    // let mut tables: Vec<
    //     Result<HashMap<String, &Arc<dyn arrow_array::Array>>, Box<dyn std::error::Error>>,
    // > = vec![];

    // Send each statement to the appropriate handler, then store the results (Result<Table, Err>
    // in tables)
    for statement in &statements {
        match statement {
            Statement::Query(query) => {
                if let ast::SetExpr::Select(sel) = &*query.body {
                    // Create Record Batch Reader
                    let table_name = sel.from[0].relation.to_string();
                    let path = format!("tables/{}.parquet", table_name);
                    let file = File::open(path).unwrap();
                    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
                        .unwrap()
                        .build()
                        .unwrap();
                    let record_batch = reader.next().unwrap().unwrap();
                    let table = handle_select(&sel, &record_batch);
                    println!("table: {:?}", table);
                    // tables.push();
                }
            }
            Statement::CreateTable { .. } => {
                // println!("found createtable {:?}, {:?}", name, columns);
                println!("found create table")
            }
            _ => println!("Only Statement::Query implemented"),
        }
    }
    // println!("tables: {:?}: ", tables);
}

fn handle_select<'a>(
    select_statement: &'a Box<sqlparser::ast::Select>,
    record_batch: &'a RecordBatch,
) -> Result<
    HashMap<&'a std::string::String, &'a Arc<dyn arrow_array::Array>>,
    Box<dyn std::error::Error>,
> {
    let columns = &select_statement.projection;

    let mut txt_cols: Vec<&String> = vec![];

    for column in columns {
        match column {
            SelectItem::UnnamedExpr(exp) => {
                if let ast::Expr::Identifier(ident) = exp {
                    txt_cols.push(&ident.value);
                }
            }
            SelectItem::Wildcard(_w) => return get_table(&vec![], true, &record_batch),
            _ => println!("found neither exp nor wildcard"),
        }
    }

    get_table(&txt_cols, false, &record_batch)
}

fn get_table<'a>(
    columns: &Vec<&'a String>,
    _wildcard: bool,
    record_batch: &'a RecordBatch,
) -> Result<
    HashMap<&'a std::string::String, &'a Arc<dyn arrow_array::Array>>,
    Box<dyn std::error::Error>,
> {
    let mut return_table = HashMap::new();

    for col_name in columns {
        let recordbatch_column = record_batch.column_by_name(col_name);
        println!("recordbatch_column: {:?}", recordbatch_column);
        return_table.insert(*col_name, recordbatch_column.unwrap());
    }
    Ok(return_table)
}
