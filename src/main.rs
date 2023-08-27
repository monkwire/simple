use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use sqlparser::ast::{visit_statements, Statement};
use sqlparser::dialect::GenericDialect;
use std::fs::File;
use std::ops::ControlFlow;

fn create_file() {
    let schema = Schema::new(vec![
        Field::new("teacher_id", DataType::Int32, false),
        Field::new("teacher_name", DataType::Utf8, true),
        Field::new("teacher_subject", DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Smith", "Johnson", "Schwartz"])),
            Arc::new(StringArray::from(vec!["History", "Math", "English"])),
        ],
    )
    .unwrap();

    let file = File::create("teachers.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    let _res = writer.write(&batch);
    writer.close().unwrap();
}

pub fn parse(sql: &str) {
    // Generate AST, identify statement type, then pass off function to appropriate parser.

    let unparsed_statements =
        sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    let mut i = 1;
    for statement in &unparsed_statements {
        println!("\nstatement {}", i);
        println!("statements: {:?}", statement);
        i += 1;

        match statement {
            Statement::Query(query) => println!("found query"),
            _ => println!("found not query"),
        }
    }

    let mut statements = vec![];
    visit_statements(&unparsed_statements, |stmt| {
        statements.push(format!("STATEMENT: {}", stmt));
        ControlFlow::<()>::Continue(())
    });
}

#[tokio::main]
async fn main() {
    println!("Hello from main");

    create_file();

    let sql_query = "SELECT * FROM teachers;";
    let res = parse(sql_query);
    println!("return val from read_file: {:?}", res);
}
