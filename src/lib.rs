#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use parquet::arrow::ArrowWriter;
    use std::fs::{File, self};
    use std::sync::Arc;
    use std::path::Path;

    pub mod parser;

    #[test]
    fn load_tests() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }}

    // fn create_file() {
    //     let schema = Schema::new(vec![
    //         Field::new("number_col_1", DataType::Int32, false),
    //         Field::new("number_col_2", DataType::Int32, false),
    //         Field::new("number_col_3", DataType::Int32, true),
    //     ]);
    //
    //     let my_vec = vec![
    //         Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
    //         Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
    //         Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
    //     ];
    //
    //     let batch = RecordBatch::try_new(Arc::new(schema.clone()), my_vec).unwrap();
    //
    //     let file = File::create("tables/numbers.parquet").unwrap();
    //     let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    //     let res = writer.write(&batch);
    //     writer.close().unwrap();
    //     println!("File created succesfully");
    //     // println!("write res: {:?}", res);
    // }

