use arrow::datatypes::SchemaRef;
use arrow_array::types::ByteArrayType;
use arrow_array::ArrayRef;
use arrow_array::Int32Array;
use arrow_array::StringArray;
use arrow_schema::Schema;
use parquet::data_type::FixedLenByteArray;
use parquet::data_type::FixedLenByteArrayType;
use parquet::data_type::Int32Type;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::file::writer::TrackedWrite;
use parquet::schema::types::SchemaDescriptor;
use parquet::{file::properties::WriterProperties, schema::types::Type};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
};

pub fn insert(path: &str) -> Result<(), std::io::Error> {
    // Open file as read and write

    let mut file_read = File::options()
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;

    let mut file_write = file_read.try_clone().unwrap();

    // Create a `SchemaDescriptor` from the parsed `Type`.
    let reader = SerializedFileReader::new(file_read).unwrap();
    let parquet_metadata = parquet::file::reader::FileReader::metadata(&reader);
    let schema = parquet_metadata.file_metadata().schema();
    println!("schema: {:?}\n", schema);

    // Set next write pointer 8 bytes from the end. This overwrites the metadata at the end.
    file_write.seek(SeekFrom::End(-8))?;

    // Create group_ writer, which can write to
    let mut writer = SerializedFileWriter::new(
        file_write,
        Arc::new(schema.clone()),
        Arc::new(WriterProperties::new()),
    );

    // Manually add appropriate rows
    // let new_cols = vec![
    //     Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
    //     Arc::new(StringArray::from(vec!["James", "Johson", "Smith"])) as ArrayRef,
    //     Arc::new(StringArray::from(vec!["spanish", "science", "english"])) as ArrayRef,
    // ];

    let mut file_writer = writer.unwrap();
    let mut row_group_writer = file_writer.next_row_group().unwrap();
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        println!("in col_writer");

        let vals: [i32; 3] = [11, 12, 13];
        col_writer
            .typed::<Int32Type>()
            .write_batch(&vals, None, None)?;
        let col_writer_res = col_writer.close();
        println!("col_writer_res: {:?}", col_writer_res);

        println!("==================\n");
    };
    let row_group_writer_res = row_group_writer.close();
    println!("row_group_writer_res: {:?}", row_group_writer_res);
    let file_writer_res = file_writer.close();
    println!("file_writer_res: {:?}", file_writer_res);

    Ok(())
}

// fn write_column() {}
//
// fn make_row_group() {}
//
// fn write_to_file() {}
