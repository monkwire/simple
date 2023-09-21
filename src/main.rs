use std::fs::File;
use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::ArrayRef;
use inserter::inserter::insert;
use parquet::arrow::arrow_writer::ArrowWriter;
mod parser;
// use parser::parser::parse;
mod inserter;

fn create_file() {
    let schema = Schema::new(vec![
        Field::new("teacher_id", DataType::Int32, false),
        Field::new("teacher_name", DataType::Utf8, false),
        Field::new("teacher_subject", DataType::Utf8, true),
    ]);

    let my_vec = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Jones", "Johson", "Smith"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["spanish", "science", "english"])) as ArrayRef,
    ];

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), my_vec).unwrap();

    let file = File::create("tables/teachers.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    let res = writer.write(&batch);
    writer.close().unwrap();
    println!("File created succesfully");
    println!("write res: {:?}", res);
    let insert_res = insert("tables/teachers.parquet");
    println!("insert res: {:?}", insert_res);
}


fn main() {
    create_file();
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
