use ::std::fmt;
use ::std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, RecordBatch};
use parquet::arrow::ArrowWriter;
use std::error::Error;
use std::fmt::format;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;

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
    let mut dir_len = 0;

    if let Ok(mut dir_tables) = std::fs::read_dir(format!("./tables/{}", table_name)) {
        println!("in directory tables/{}/", table_name);
        dir_len = dir_tables.by_ref().count();
        println!("dir count: {}", dir_len);
    } else {
        if fs::create_dir(format!("./tables/{}/", table_name)).is_err() {
            let err = Err(CreateError::WriteError(WriteError {
                description: String::from(format!(
                    "Could not find or create tables/{} directory.",
                    table_name
                )),
            }));
            return err;
        }
    }

    let batch = RecordBatch::try_new(Arc::new(schema), cols).unwrap();
    let file = File::create(format!(
        "tables/{}/{}_{}.parquet",
        table_name,
        table_name,
        dir_len + 1
    ));

    if let Ok(file) = file {
        dir_len = std::fs::read_dir(&format!("./tables/{}", table_name))
            .unwrap()
            .by_ref()
            .count();
        println!("attempting to write file {}", dir_len);
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        let res = writer.write(&batch);
        if let Ok(_res) = res {
            writer.close().unwrap();
            println!(
                "succesfully wrote file {}, directory now has {} files.",
                dir_len, dir_len
            );
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
    use arrow::{buffer::Buffer, compute::kernels::zip};
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
        let table_name = "test_table";
        if std::fs::read_dir(format!("./tables/{}", table_name)).is_ok() {
            if remove_dir_all(format!("./tables/{}", table_name)).is_err() {
                panic!("Cannot set up create_new_table test")
            }
        }

        let test_schema = Schema::new(vec![Field::new("number_col_3", DataType::Int32, true)]);
        let test_rows = vec![Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef];

        assert!(std::fs::read_dir(format!("./tables/{}", table_name)).is_err());
        assert!(create(table_name, test_schema, test_rows).is_ok());
        let dir_files = std::fs::read_dir(format!("./tables/{}", table_name));
        assert!(dir_files.is_ok());
        if let Ok(mut f) = dir_files {
            let created = f.next();
            if let Ok(cf) = created.unwrap() {
                assert_eq!(cf.file_name(), "test_table_1.parquet");
            } else {
                assert!(false);
            }
        }
    }

    #[test]
    fn create_multiple_tables() {
        let table_name = "test_multiple_tables";
        if std::fs::read_dir(format!("./tables/{}", table_name)).is_ok() {
            if remove_dir_all(format!("./tables/{}", table_name)).is_err() {
                panic!("Cannot set up create_new_table test")
            }
        }

        for i in 1..6 {
            let test_schema = Schema::new(vec![Field::new("number_col_3", DataType::Int32, true)]);
            let test_rows = vec![Arc::new(Int32Array::from(vec![i, i, i])) as ArrayRef];

            assert!(create(table_name, test_schema, test_rows).is_ok());
            let dir_files = std::fs::read_dir(format!("./tables/{}", table_name));
            assert!(dir_files.is_ok());

            if let Ok(files) = dir_files {
                let mut file_names: Vec<String> = files
                    .map(|f| match f {
                        Ok(file) => file.file_name().into_string().unwrap(),
                        _ => String::from(""),
                    })
                    .filter(|el| el.len() > 0)
                    .collect::<Vec<String>>()
                    .try_into()
                    .unwrap();

                assert_eq!(file_names.len(), usize::try_from(i).unwrap());
                file_names.sort();
                assert_eq!(
                    file_names.last().unwrap(),
                    &format!("{}_{}.parquet", table_name, i)
                );
            } else {
                assert!(false);
            }
        }
    }
}
