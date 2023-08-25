use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::ParquetReadOptions;
use datafusion::{self, prelude::SessionContext};
use parquet::arrow::arrow_writer::ArrowWriter;
use std::fs::File;

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

async fn read_file(sql: &str) -> Result<(), DataFusionError> {
    println!("hello from top of read_file");
    let ctx = SessionContext::new();

    let df_future = ctx
        .read_parquet(
            "./teachers.parquet",
            ParquetReadOptions {
                file_extension: ".parquet",
                table_partition_cols: vec![
                    ("teacher_id".to_string(), DataType::Int32),
                    ("teacher_name".to_string(), DataType::Utf8),
                    ("teacher_subject".to_string(), DataType::Utf8),
                ],
                parquet_pruning: None,
                skip_metadata: None,
            },
        )
        .await?;

    // let table_provider = ctx.register_table("teachers", Arc::new(df_future));

    // println!("df_future: {:?}", df_future);

    let df = ctx.sql(sql).await?;
    // println!("df: {:?}", df);

    let results = df.collect().await?;

    println!("Reading file");
    println!("results {:?}", results);

    Ok(())

    // println!("df: {:?}", df);
    // let batches = df.collect();
    // for batch in batches {
    //     println!("{:?}", batch);
    // }
}

#[tokio::main]
async fn main() {
    println!("Hello from main");

    create_file();

    let sql_query = "SELECT * FROM teachers";
    let res = read_file(sql_query).await;
    println!("return val from read_file: {:?}", res);
}
