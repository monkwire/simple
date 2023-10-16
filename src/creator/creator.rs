use ::std::fmt;
use arrow::datatypes::Schema as ArrowSchema;
use arrow_array::ArrayRef;
use std::error::Error;
use std::fs;

#[derive(Debug, PartialEq, Clone)]
pub enum CreateError {
    WriteError(WriteError),
}
#[derive(Debug, PartialEq, Clone)]
pub struct WriteError {
    description: String,
}

impl fmt::Display for CreateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateError::WriteError(e) => write!(f, "Write Error: {}", e.description),
            _ => write!(f, ""),
        }
    }
}

impl Error for CreateError {}

pub fn create(
    table_name: &str,
    schema: ArrowSchema,
    rows: Vec<ArrayRef>,
) -> Result<String, CreateError> {
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

    Ok(String::from("return not imple for create"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use arrow::buffer::Buffer;

    use super::*;
    #[test]
    fn load_create_tests() {
        assert!(true);
    }
}
