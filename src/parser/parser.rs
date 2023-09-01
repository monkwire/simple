use arrow::array::ArrayData;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlparser::ast::{self, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use std::fs::File;

pub fn parse(
    sql: &str,
) -> Vec<Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>>> {
    let statements = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    let mut tables = Vec::new();

    for statement in &statements {
        match statement {
            Statement::Query(query) => {
                if let ast::SetExpr::Select(sel) = &*query.body {
                    tables.push(handle_select(&sel));
                }
            }
            Statement::CreateTable { .. } => {
                println!("found create table")
            }
            _ => println!("Only Statement::Query implemented"),
        }
    }
    tables
}

fn handle_select<'a>(
    select_statement: &'a Box<sqlparser::ast::Select>,
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
            SelectItem::Wildcard(_w) => {
                return get_table(
                    &select_statement.from[0].relation.to_string(),
                    &vec![],
                    true,
                )
            }
            _ => println!("found neither exp nor wildcard"),
        }
    }

    get_table(
        &select_statement.from[0].relation.to_string(),
        &txt_cols,
        false,
    )
}

fn get_table<'a>(
    table_name: &str,
    columns: &Vec<&'a String>,
    _wildcard: bool,
) -> Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>> {
    let path = format!("tables/{}.parquet", table_name);
    let file = File::open(path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let record_batch = reader.next().unwrap().unwrap();

    let mut return_table = HashMap::new();

    for col_name in columns {
        let recordbatch_column = record_batch.column_by_name(col_name);
        return_table.insert(col_name.to_string(), recordbatch_column.unwrap().to_data());
    }
    println!("{}", "-".repeat(50));
    Ok(return_table)
}
