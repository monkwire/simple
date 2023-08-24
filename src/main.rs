use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::ParquetReadOptions;
use datafusion::{self, prelude::SessionContext};

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
    );
}

fn read_file(sql: &str) {
    let ctx = SessionContext::new();

    ctx.read_parquet(
        "teachers",
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
    );

    let df = ctx.sql(sql);
}

fn main() {}
