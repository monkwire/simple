// use arrow::record_batch::RecordBatch;
use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, Int32Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
// use parquet::arrow::arrow_writer::ArrowWriter;
// use parquet::file::properties::WriterProperties;
// use parquet::arrow::arrow_reader;
use sqlparser::ast::{self, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::fs::File;
// use std::sync::Arc;

// Top level parser
// Retuns Optional Table
// If table exists, returns table as requested in a select, or table with newly insterted data,
// or empty table

pub fn parse(sql: &str) {
    // Generate AST, identify statement type, then pass off function to appropriate parser.
    println!("hello from parse");

    let unparsed_statements =
        sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    for statement in &unparsed_statements {
        // println!("statements: {:?}\n", statement);

        match statement {
            Statement::Query(query) => match *query.body.clone() {
                ast::SetExpr::Select(sel) => {
                    println!("found select: {:?}", sel);
                    handle_select(&sel);
                }
                _ => println!("did not find select"),
            },
            _ => println!("query not found"),
        }
    }
}

fn handle_select(select_statement: &Box<sqlparser::ast::Select>) {
    println!("\nHello from handle_select");
    let columns = &select_statement.projection;

    let mut txt_cols: Vec<&String> = vec![];
    let tables = &select_statement.from;
    let table = tables[0].relation.to_string();

    for column in columns {
        match column {
            SelectItem::UnnamedExpr(exp) => {
                match exp {
                    ast::Expr::Identifier(ident) => {
                        println!("found ident: {:?}", ident.value);
                        let val: &String = &ident.value;
                        txt_cols.push(val);
                    }
                    _ => println!("did not find ident"),
                }
                println!("found unamed exp: {:?}", exp);
            }
            SelectItem::Wildcard(wild) => {
                println!("found wildcard: {}", wild);
            }
            _ => println!("found neither exp nor wildcard"),
        }
    }

    println!("columns: {:?}\n", columns);
    println!("txt_cols: {:?}\n", txt_cols);
    println!("table: {:?}\n ", table);

    let _res = get_table(&table, "teachers.parquet", &txt_cols);
}

fn get_table(
    table_name: &str,
    path: &str,
    columns: &Vec<&String>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nhello from get_table");
    println!(
        "table_name: {}, path: {}, columns: {:?}",
        table_name, path, columns
    );

    let file = File::open(path)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    let record_batch = reader.next().unwrap().unwrap();

    println!("Read {} records.", record_batch.num_rows());
    println!("{:?}", record_batch);

    Ok(())
}
