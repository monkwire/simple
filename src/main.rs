use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::{BooleanArray, Float32Array};
use parquet::arrow::arrow_writer::ArrowWriter;
mod parser;
use parser::parser::parse;
use std::fs::File;

fn create_file() {
    let schema = Schema::new(vec![
        Field::new("food_name", DataType::Utf8, false),
        Field::new("PLU", DataType::Int32, false),
        Field::new("price", DataType::Float32, true),
        Field::new("qty", DataType::Int32, false),
        Field::new("organic", DataType::Boolean, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec![
                "Apple", "Orange", "Pear", "Tomato", "Tomato",
            ])),
            Arc::new(Int32Array::from(vec![3000, 3027, 3012, 3061, 3423])),
            Arc::new(Float32Array::from(vec![2.05, 1.32, 2.12, 1.75, 2.60])),
            Arc::new(Int32Array::from(vec![89, 42, 30, 64, 24])),
            Arc::new(BooleanArray::from(vec![false, false, true, false, false])),
        ],
    )
    .unwrap();

    let file = File::create("tables/foods.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    let _res = writer.write(&batch);
    writer.close().unwrap();
}

fn main() {
    create_file();
    let sql_queries = vec![
        "SELECT teacher_name, teacher_id FROM teachers;",
        "SELECT * FROM teachers;",
        "SELECT teacher_id FROM teachers; SELECT teacher_subject FROM teachers;",
        "SELECT * FROM foods;",
        "CREATE TABLE books (ISBN CHAR PRIMARY KEY, Title CHAR NOT NULL, Author CHAR NOT NULL, Genre CHAR NOT NULL);",
    ];
    for sql_query in sql_queries {
        let res = parse(sql_query, "../teachers.parquet");
        println!("\n parsing: {}:\n{:?}", sql_query, res);
    }
}

//
//
//
//
// "INSERT INTO books (ISBN, Title, Author, Genre) VALUES
//     ('978-1234567890', 'The Great Adventure', 'John Doe', 'Adventure'),
//     ('978-0987654321', 'Mysteries of the Unknown', 'Jane Smith', 'Mystery'),
//     ('978-1122334455', 'Programming for Beginners', 'Alan Turing', 'Technology'),
//     ('978-5566778899', 'The Cosmic Dance', 'Stephen Hawking', 'Science');",
