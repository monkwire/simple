use ::std::fmt;
use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, RecordBatch};
use parquet::arrow::ArrowWriter;
use std::error::Error;
use std::fs;
use std::fs::File;

#[derive(Debug, PartialEq, Clone)]
pub enum CreateError {
    WriteError(WriteError),
    DirectoryError(DirectoryError),
}
#[derive(Debug, PartialEq, Clone)]
pub struct WriteError {
    description: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DirectoryError {
    description: String,
}

impl fmt::Display for CreateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateError::WriteError(e) => write!(f, "Write Error: {}", e.description),
            CreateError::DirectoryError(e) => write!(f, "DirectoryError: {}", e.description),
            _ => write!(f, ""),
        }
    }
}

impl Error for CreateError {}

pub fn create(
    table_name: &str,
    schema: ArrowSchema,
    cols: Vec<ArrayRef>,
) -> Result<(), CreateError> {
    if let Ok(_dir_tables) = std::fs::read_dir(format!("./tables/{}", table_name)) {
        println!("found directory tables/{}", table_name);
    } else {
        if fs::create_dir(format!("./tables/{}", table_name)).is_err() {
            let err = Err(CreateError::WriteError(WriteError {
                description: String::from(format!(
                    "Could not find or create tables/{} directory.",
                    table_name
                )),
            }));
            return err;
        } else {
            println!("created new directory");
        }
    }

    let batch = RecordBatch::try_new(Arc::new(schema), cols).unwrap();
    let file = File::create(format!(
        "tables/{}/{}_{}.parquet",
        table_name, table_name, 1
    ));

    if let Ok(file) = file {
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        let res = writer.write(&batch);
        if let Ok(_res) = res {
            writer.close().unwrap();
            return Ok(());
        }
    } else {
        return Err(CreateError::DirectoryError(DirectoryError {
            description: String::from("Failed to create file."),
        }));
    }
    Err(CreateError::WriteError(WriteError {
        description: String::from("Failed to write to table."),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::Buffer;
    use std::{
        fmt::format,
        fs::{self, remove_dir_all},
    };

    #[test]
    fn load_create_tests() {
        assert!(true);
    }

    #[test]
    fn create_new_table() {
        let table_name = "test_table_1";
        if std::fs::read_dir(format!("./tables/{}", table_name)).is_ok() {
            if remove_dir_all(format!("./tables/{}", table_name)).is_err() {
                panic!("Cannot set up create_new_table test")
            }
        }

        let test_schema = Schema::new(vec![Field::new("number_col_3", DataType::Int32, true)]);
        let test_rows = vec![Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef];

        assert!(std::fs::read_dir(format!("./tables/{}", table_name)).is_err());
        assert!(create(table_name, test_schema, test_rows).is_ok());
    }
}
