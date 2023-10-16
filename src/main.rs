use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::ArrayRef;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::fs::File;
use std::io::Write;
mod parser;
use creator::creator::create;
use parser::parser::parse;
mod creator;

fn create_file() {
    let schema = Schema::new(vec![
        Field::new("number_col_1", DataType::Int32, false),
        Field::new("number_col_2", DataType::Int32, false),
        Field::new("number_col_3", DataType::Int32, true),
    ]);

    let my_vec = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
        Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
    ];

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), my_vec).unwrap();

    let file = File::create("tables/numbers.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    let res = writer.write(&batch);

    println!("schema: {:?}", schema);
    writer.close().unwrap();
    println!("File created succesfully\n");
    // println!("write res: {:?}", res);
}

fn main() {
    let music_schema = Schema::new(vec![
        Field::new("number_col_1", DataType::Int32, false),
        Field::new("number_col_2", DataType::Int32, false),
        Field::new("number_col_3", DataType::Int32, true),
    ]);

    let my_vec = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
        Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
    ];

    println!("{:?}", create("music_table", music_schema, my_vec));
}

//     let sql_queries = vec![
//         //         "CREATE TABLE employees (
//         //     employee_id INT PRIMARY KEY,
//         //     first_name VARCHAR(50),
//         //     last_name VARCHAR(50),
//         //     date_of_birth DATE,
//         //     email VARCHAR(100)
//         //
//         // );",
//         // "SELECT * FROM employees;", // "SELECT teacher_name, teacher_id FROM teachers;",
//         "SELECT * FROM teachers;",
//         // "SELECT teacher_id FROM teachers; SELECT teacher_subject FROM teachers;",
//         // "SELECT * FROM foods;",
//         // "CREATE TABLE books(
//         //     ISBN VARCHAR(13) PRIMARY KEY,
//         //         Title VARCHAR(255) NOT NULL,
//         //         Author VARCHAR(255) NOT NULL,
//         //         Genre VARCHAR(100) NOT NULL
//         // );",
//         // "SELECT ISBN, Title, Author, Genre FROM books;",
//         // "INSERT INTO books (ISBN, Title, Author, Genre) VALUES
//         //         ('978-1234567890', 'The Great Adventure', 'John Doe', 'Adventure'),
//         //         ('978-0987654321', 'Mysteries of the Unknown', 'Jane Smith', 'Mystery'),
//         //         ('978-1122334455', 'Programming for Beginners', 'Alan Turing', 'Technology'),
//         //         ('978-5566778899', 'The Cosmic Dance', 'Stephen Hawking', 'Science');
//         // ",
//         // "SELECT * FROM books;",
//         // "INSERT INTO teachers (teacher_id, teacher_name, teacher_subject) VALUES ('4', 'Robinson', 'Anthropology'), ('5', 'Irving', 'German');",
//         // "SELECT * FROM teachers;",
//
//         // "SELECT * FROM teachers;",
//     ];
//     for sql_query in sql_queries {
//         let res = parse(sql_query);
//         println!("{}", "=".repeat(50));
//         println!("\n RAW SQL: '{}'\n", sql_query);
//         println!("res: {:?}\n", res);
//     }
// }

//
//
//
//
// "INSERT INTO books (ISBN, Title, Author, Genre) VALUES
//     ('978-1234567890', 'The Great Adventure', 'John Doe', 'Adventure'),
//     ('978-0987654321', 'Mysteries of the Unknown', 'Jane Smith', 'Mystery'),
//     ('978-1122334455', 'Programming for Beginners', 'Alan Turing', 'Technology'),
//     ('978-5566778899', 'The Cosmic Dance', 'Stephen Hawking', 'Science');",
