use arrow::array::{self, ArrayData};
use arrow::datatypes::DataType;
use arrow_array::{Int32Array, PrimitiveArray, StringArray};
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
                    let table = handle_select(&sel);
                    let table_string = generate_table_string(table.unwrap());
                    // tables.push(table);
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

    Ok(return_table)
}

fn generate_table_string(arraydata: HashMap<String, ArrayData>) {
    println!("handle_array_data: {:?}", arraydata);

    let table_string = String::new();

    for (col_name, arr) in &arraydata {
        println!("arr datatype: {:?}", arr.data_type());
        match arr.data_type() {
            DataType::Utf8 => {
                let string_array = StringArray::from(arr.clone());
                println!("{:?}", string_array);
            }
            DataType::Int32 => {
                let int_array = Int32Array::from(arr.clone());
                println!("{:?}", int_array);
            }
            _ => println!("unsupported"),
        };
    }
}
