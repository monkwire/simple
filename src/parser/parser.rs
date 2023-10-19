use ::std::fmt;
use arrow::array::ArrayData;
use arrow::datatypes::DataType as arrow_datatype;
use arrow::datatypes::{Field, Schema};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
pub(crate) use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use sqlparser::ast::Query;
use sqlparser::ast::{self, ColumnDef, DataType, Ident, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::sync::Arc;

use crate::creator::creator::{create, CreateError, DirectoryError};

#[derive(Debug, PartialEq, Clone)]
pub enum ParseError {
    TableNotFound(TableNotFoundStruct),
    BadSQL(BadSQL),
    Unsupported(UnsupportedFunction),
}
#[derive(Debug, PartialEq, Clone)]
pub struct TableNotFoundStruct {
    description: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct BadSQL {
    description: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnsupportedFunction {
    description: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::TableNotFound(e) => write!(f, "Table Not Found: {}", e.description),
            ParseError::BadSQL(e) => write!(f, "BadSQL Error: {}", e.description),
            _ => write!(f, ""),
        }
    }
}

impl ParseError {
    fn loose_eq(&self, other: ParseError) -> bool {
        match self {
            ParseError::TableNotFound(e1) => match other {
                ParseError::TableNotFound(e2) => {
                    if !e1.description.ends_with(".parquet")
                        || !e2.description.ends_with(".parquet")
                    {
                        return false;
                    }
                    let prefix_original = e1
                        .description
                        .trim_end_matches(".parquet")
                        .rsplitn(2, '_')
                        .nth(1)
                        .unwrap_or("");
                    let prefix_other = e2
                        .description
                        .trim_end_matches(".parquet")
                        .rsplitn(2, '_')
                        .nth(1)
                        .unwrap_or("");

                    return prefix_original == prefix_other;
                }
                _ => return false,
            },
            _ => return false,
        }
    }
}

impl Error for ParseError {}

pub fn parse(sql: &str) -> Vec<Result<HashMap<std::string::String, ArrayData>, ParseError>> {
    let statements_res = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql);
    if let Ok(statements) = statements_res {
        let mut tables = Vec::new();
        let mut query_results = Vec::new();

        for statement in &statements {
            match statement {
                Statement::Query(query) => {
                    if let ast::SetExpr::Select(sel) = &*query.body {
                        let query_res = handle_select(&sel);
                        query_results.push(query_res);
                    }
                }
                Statement::CreateTable {
                    name,
                    columns,
                    query,
                    ..
                } => {
                    handle_create_table(&name.to_string());
                    let table = get_table(&name.to_string(), Vec::new(), true);
                    tables.push(table);
                    let query_result = get_table(&name.to_string(), Vec::new(), true);
                    query_results.push(query_result);
                }
                Statement::Insert {
                    or,
                    into,
                    table_name,
                    columns,
                    overwrite,
                    source,
                    partitioned,
                    after_columns,
                    table,
                    on,
                    returning,
                } => {
                    let err = ParseError::Unsupported(UnsupportedFunction {
                        description: String::from("Insert not supported."),
                    });
                    query_results.push(Err(err));
                }
                _ => query_results.push(Err(ParseError::Unsupported(UnsupportedFunction {
                    description: "Query type not implemented.".to_string(),
                }))),
            }
        }
        return query_results;
    } else {
        let err = Err(ParseError::BadSQL(BadSQL {
            description: format!("Could not parse {}", sql),
        }));
        return vec![err];
    }
}

fn handle_select<'a>(
    select_statement: &'a Box<sqlparser::ast::Select>,
) -> Result<HashMap<std::string::String, ArrayData>, ParseError> {
    let columns = &select_statement.projection;

    let mut column_names: Vec<String> = vec![];

    for column in columns {
        match column {
            SelectItem::UnnamedExpr(exp) => {
                if let ast::Expr::Identifier(ident) = exp {
                    column_names.push(ident.value.clone());
                }
            }
            SelectItem::Wildcard(_w) => {
                return get_table(&select_statement.from[0].relation.to_string(), vec![], true)
            }
            _ => println!("found neither exp nor wildcard"),
        }
    }

    get_table(
        &select_statement.from[0].relation.to_string(),
        column_names,
        false,
    )
}

fn get_all_column_names(table_name: &str) -> Vec<String> {
    let path = format!("tables/{}.parquet", table_name);
    let mut columns = Vec::new();
    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema();
        for field in schema.get_fields().iter() {
            columns.push(field.name().to_string());
        }
    }

    columns
}

fn get_table<'a>(
    table_name: &str,
    columns: Vec<String>,
    wildcard: bool,
) -> Result<HashMap<std::string::String, ArrayData>, ParseError> {
    if wildcard {
        get_table(table_name, get_all_column_names(table_name), false)
    } else {
        let path = format!("tables/{}/{}.parquet", table_name, table_name);
        let mut return_table = HashMap::new();

        let table_dir = std::fs::read_dir(&path);
        if table_dir.is_err() {
            return Err(ParseError::TableNotFound(TableNotFoundStruct {
                description: String::from(format!("Could not open {}", &path)),
            }));
        }
        for i in 1..=table_dir.unwrap().by_ref().count() {
            let file_path = format!("tables/{}/{}_{}.parquet", table_name, table_name, i);
            if let Ok(file) = File::open(file_path) {
                let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap();

                let next_reader = reader.next();
                if let Some(record_batch_option) = next_reader {
                    if let Ok(record_batch) = record_batch_option {
                        let schema = record_batch.schema();
                        for col_name in &columns {
                            let recordbatch_column = record_batch.column_by_name(&col_name);
                            return_table.insert(
                                col_name.to_string(),
                                recordbatch_column.unwrap().to_data(),
                            );
                        }
                    }
                }
                // let record_batch = reader.next().unwrap().unwrap();
            } else {
                let err = Err(ParseError::TableNotFound(TableNotFoundStruct {
                    description: (format!(
                        "Could not find {} in {}.",
                        String::from(table_name),
                        "tables"
                    )),
                }));
                println!("returning from get_table:\n {:?}", err);
                return err;
            }
        }
        return Ok(return_table);
    }
}

fn handle_create_table(table_name: &str) -> Result<(), ParseError> {
    return Err(ParseError::Unsupported(UnsupportedFunction {
        description: String::from("Handle create not set up"),
    }));
}

fn generate_table_string(arraydata: HashMap<String, ArrayData>) {
    let _table_string = String::new();

    for (_col_name, arr) in &arraydata {
        match arr.data_type() {
            arrow_datatype::Utf8 => {
                let _string_array = StringArray::from(arr.clone());
            }
            arrow_datatype::Int32 => {
                let _int_array = Int32Array::from(arr.clone());
            }
            _ => println!("unsupported"),
        };
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TestError {
    description: String,
}

pub fn create_test_file(table_name: &str) -> Result<(), TestError> {
    let mut dir_len = 0;

    if let Ok(mut dir_tables) = std::fs::read_dir(format!("./tables/{}", table_name)) {
        dir_len = dir_tables.by_ref().count();
        println!("dir count: {}", dir_len);
    } else {
        if fs::create_dir(format!("./tables/{}/", table_name)).is_err() {
            return Err(TestError {
                description: String::from("Could not create directory for tables."),
            });
        }
    }

    let schema = Schema::new(vec![
        Field::new("col_1", arrow_datatype::Int32, false),
        Field::new("col_2", arrow_datatype::Int32, false),
        Field::new("col_3", arrow_datatype::Int32, true),
    ]);

    let cols = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
        Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
    ];

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
        } else {
            return Err(TestError {
                description: String::from("Unsuccessful Write."),
            });
        }
    } else {
        return Err(TestError {
            description: String::from("Could not create file."),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::buffer::Buffer;
    use std::fs::{self, remove_dir_all};

    use super::*;
    #[test]
    fn parse_empty() {
        let empty_parse_res = parse("");
        assert_eq!(empty_parse_res.len(), 0);
    }

    #[test]
    fn parse_query_for_bad_table() {
        let table_name = "nonexistanttable";
        let parse_res = parse(&format!("SELECT * FROM {};", table_name));
        assert_eq!(parse_res.len(), 1, "parse('SELECT * FROM {}') should return a vec with one result. Instead returning a vec of size {}", table_name, parse_res.len());
        assert!(
            parse_res[0].is_err(),
            "Queries with inncorrect SQL should result in an error."
        );

        match &parse_res[0] {
            Err(parse_error) => match parse_error {
                ParseError::TableNotFound(err) => assert!(true),
                _ => panic!("Expected TableNotFound error."),
            },
            _ => panic!("Expected TableNotFound error."),
        }
    }

    #[test]
    fn parse_bad_sql() {
        let query_1 = "SELECT table_name FROM *;";
        let parse_res = parse(&query_1);
        assert_eq!(
            parse_res.len(),
            1,
            "parse called with a non-empty string should return a vec with at least one Result"
        );
        assert!(
            parse_res[0].is_err(),
            "parse called with incorrect SQL should return Vec<Err>."
        );

        match &parse_res[0] {
            Err(parse_err) => match parse_err {
                ParseError::BadSQL(err) => {
                    assert_eq!(err.description, format!("Could not parse {}", query_1))
                }
                _ => panic!("Expected BadSQL."),
            },
            _ => panic!("Expected BadSQL."),
        }

        let table_name = "test_table3";

        create_test_file(table_name);
        let directory_cleanup_res = fs::remove_dir_all(format!("tables/{}", table_name));
        if directory_cleanup_res.is_err() {
            panic!("Could not remove test directory");
        }

        let query_2 = &format!(
            "SELECT * FROM {}; aslkdjwqeu col_1 FROM {};",
            table_name, table_name
        );
        let res = parse(query_2);

        assert_eq!(res.len(), 1);
        assert!(res[0].is_err());

        match &res[0] {
            Err(parse_err) => match parse_err {
                ParseError::BadSQL(err) => {
                    assert_eq!(err.description, format!("Could not parse {}", query_2))
                }
                _ => panic!("Expected BadSQL."),
            },
            _ => panic!("Expected BadSQL."),
        }

        // Add cleanup function
    }

    #[test]
    fn parse_good_sql() {
        let table_name = "test_table";

        assert!(create_test_file(table_name).is_ok());
        if std::fs::read_dir(format!("./tables/{}", table_name)).is_ok() {
            if remove_dir_all(format!("./tables/{}", table_name)).is_err() {
                panic!("Cannot set up create_new_table test")
            }
        }

        let directory_cleanup_res = fs::remove_dir_all(format!("tables/{}", table_name));
        if directory_cleanup_res.is_err() {
            panic!("Could not remove test directory");
        }

        let parse_res = parse(&format!("SELECT * FROM {};", table_name));
        assert_eq!(
            parse_res.len(),
            1,
            "parse called with a non-empty string should return a vec with at least one Result"
        );
        assert!(
            parse_res[0].is_ok(),
            "parse called with correct SQL code should not Err."
        );

        let res = parse_res[0].clone().unwrap();
        assert_eq!(res["col_1"].len(), 3);
        assert_eq!(res["col_2"].len(), 3);
        assert_eq!(res["col_3"].len(), 3);
    }

    #[test]
    fn parse_multiple_queries_to_same_table() {
        let table_name = "test_table2";
        assert!(create_test_file(table_name).is_ok());

        let directory_cleanup_res = fs::remove_dir_all(format!("tables/{}", table_name));
        if directory_cleanup_res.is_err() {
            panic!("Could not remove test directory");
        }

        let res = parse(&format!(
            "SELECT * FROM {}; SELECT col_1 FROM {};",
            table_name, table_name
        ));
        assert_eq!(res.len(), 2);

        assert!(res[0].is_ok());
        let res_1 = res[0].clone().unwrap();
        assert_eq!(res_1["col_1"].len(), 3);
        assert_eq!(res_1["col_2"].len(), 3);
        assert_eq!(res_1["col_3"].len(), 3);

        assert!(res[1].is_ok());
        let res_2 = res[1].clone().unwrap();
        assert_eq!(res_1["col_1"].len(), 3);

        assert!(!res_2.contains_key("col_2"));
        assert!(!res_2.contains_key("col_3"));
    }
}
