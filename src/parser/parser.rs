use arrow::array::ArrayData;
// use arrow::record_batch;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlparser::ast::{self, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use std::fs::File;
// use std::sync::Arc;

// Parse function returns a vec of the results of all SQL Statements. All successful statement
// results return tables.
//
// pub get_table_data<'a>(record_batch: &'a RecordBatch) -> Vec<String, > {
// }
//
pub fn parse(
    sql: &str,
) -> Vec<Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>>> {
    let statements = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    // Create results table
    // let mut tables: Vec<HashMap<String, dyn arrow_array::Array>> = vec![];
    // let mut tables: Vec<
    //     Result<HashMap<String, &Arc<dyn arrow_array::Array>>, Box<dyn std::error::Error>>,
    // > = vec![];
    let mut tables = Vec::new();

    // Send each statement to the appropriate handler, then store the results (Result<Table, Err>
    // in tables)
    for statement in &statements {
        match statement {
            Statement::Query(query) => {
                if let ast::SetExpr::Select(sel) = &*query.body {
                    //
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
                    // println!("table: {:?}", table);
                    tables.push(table);

                    // let mut inner_tables = vec![];
                    // r_tables.push(table);
                    // println!("inner_tables {:?}", inner_tables);
                }
            }
            Statement::CreateTable { .. } => {
                println!("found create table")
            }
            _ => println!("Only Statement::Query implemented"),
        }
    }
    // println!("tables: {:?}: ", tables);
    tables
}

fn handle_select<'a>(
    select_statement: &'a Box<sqlparser::ast::Select>,
    record_batch: &'a RecordBatch,
) -> Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>> {
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
) -> Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>> {
    let mut return_table = HashMap::new();

    for col_name in columns {
        let recordbatch_column = record_batch.column_by_name(col_name);
        return_table.insert(col_name.to_string(), recordbatch_column.unwrap().to_data());
    }
    // println!("returning from get_table: {:?}", return_table);
    // println!("returned from get_table");
    println!("{}", "-".repeat(50));
    Ok(return_table)
}
