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
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;
use parquet::{file::properties::WriterProperties, schema::types::Type};
use std::collections::HashMap;
use std::fs;
use std::io::repeat;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
};

pub fn log(path: &str, data: &str) -> Result<(), std::io::Error> {
    let mut log_file = File::create(path)?;
    log_file.write_all(data.as_bytes())?;
    Ok(())
}

pub fn insert_by_join(path_1: &str, path_2: &str) {
    // create and close file_1

    let path_1 = Path::new(path_1);
    let message_type = "
        message schema {
        REQUIRED INT32 b;
        }";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let file = fs::File::create(&path_1).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        let values: [i32; 5] = [0, 1, 2, 3, 4];

        col_writer
            .typed::<Int32Type>()
            .write_batch(&values, None, None)
            .unwrap();

        col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();
    writer.close().unwrap();

    

        
    // create file_2


    // append file_1 to file_2



    // close file_2


}


pub fn insert(path: &str) -> Result<(), std::io::Error> {
    // Open file as read and write

    let mut file_read = File::options()
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;

    let mut file_2_read = File::options()
        .read(true)
        .write(true)
        .truncate(false)
        .open("tables/numbers2.parquet")?;

                    let var: String = String::from("myvar");

    let mut file_write = file_read.try_clone().unwrap();
    let mut tracked_write = TrackedWrite::new(file_write);

    // Create a `SchemaDescriptor` from the parsed `Type`.
    let reader = SerializedFileReader::new(file_read).unwrap();
    let parquet_metadata = parquet::file::reader::FileReader::metadata(&reader);
    println!("read_file metadata: {:?}", parquet_metadata);
    log("metadata", &format!("{:?}", parquet_metadata));
    let schema = parquet_metadata.file_metadata().schema();
    println!("schema: {:?}\n", schema);

    let mut file_write_2 = file_2_read.try_clone().unwrap();
    let mut tracked_write_2 = TrackedWrite::new(file_write_2);

    // Create a `SchemaDescriptor` from the parsed `Type`.
    let reader_2 = SerializedFileReader::new(file_2_read).unwrap();

    // Manually build onclose \:
    // let mut row_groups = Vec::new();
    // let mut row_bloom_filters = Vec::new();
    // let mut row_column_indexes = Vec::new();
    // let mut row_offset_indexes = Vec::new();
    //
    // let on_close = |metadata,
    //                 row_group_bloom_filter,
    //                 row_group_column_index,
    //                 row_group_offset_index| {
    //     row_groups.push(metadata);
    //     row_bloom_filters.push(row_group_bloom_filter);
    //     row_column_indexes.push(row_group_column_index);
    //     row_offset_indexes.push(row_group_offset_index);
    //     Ok(())
    // };

    // Example usage:
    // let metadata = "metadata";
    // let bloom_filter = "bloom_filter";
    // let column_index = "column_index";
    // let offset_index = "offset_index";
    // on_close(metadata, bloom_filter, column_index, offset_index)?;

    // Create Row Group Writer
    let mut row_group_writer = SerializedRowGroupWriter::new(
        Arc::new(SchemaDescriptor::new(Arc::new(schema.clone()))),
        Arc::new(WriterProperties::new()),
        &mut tracked_write,
        parquet_metadata.num_row_groups() as i16 + (1 as i16),
        None,
    );

    // append_column approach
    // row_group_writer.append_column(&reader_2,close );

    // for i in 0..2 {
    //     println!("in loop; i: {}", i);
    //     row_group_writer.append_column(&reader, row_group_writer.close);
    //
    //         // let vals: [i32; 5] = [3, 6, 7, 8, 9];
    //         // col_writer
    //         //     .typed::<Int32Type>()
    //         //     .write_batch(&vals, Some(&[1 as i16]), Some(&[1 as i16]))?;
    //         // let col_writer_res = col_writer.close();
    //         // println!("col_writer_res: {:?}", col_writer_res);
    //
    //         println!("==================\n");
    // };

    // for i in 0..2 {
    //     println!("in loop; i: {}", i);
    //     if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
    //         println!("in col_writer");
    //
    //         let vals: [i32; 5] = [3, 6, 7, 8, 9];
    //         col_writer
    //             .typed::<Int32Type>()
    //             .write_batch(&vals, Some(&[1 as i16]), Some(&[1 as i16]))?;
    //         let col_writer_res = col_writer.close();
    //         println!("col_writer_res: {:?}", col_writer_res);
    //
    //         println!("==================\n");
    //     };
    // }
    //
    // SerializedPageWriter::new(row_group_writer.&row_group_writer);
    // let row_group_writer_on_close -
    let row_group_writer_res = row_group_writer.close();
    log(
        "post_insert_metadata.txt",
        &format!("{:?}", row_group_writer_res.unwrap()),
    );
    // println!("row_group_writer_res: {:?}", row_group_writer_res);
    // let file_writer_res = file_writer.close();
    // println!("file_writer_res: {:?}", file_writer_res);

    Ok(())
}

// fn write_column() {}
//
// fn make_row_group() {}
//
// fn write_to_file() {}
