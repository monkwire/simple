use arrow::array::ArrayData;
use arrow::datatypes::DataType as arrow_datatype;
use arrow::datatypes::{Field, Schema};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use sqlparser::ast::Query;
use sqlparser::ast::{self, ColumnDef, DataType, Ident, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::error::Error;
use::std::fmt;



#[derive(Debug, PartialEq, Clone)]
pub enum ParseError {
    TableNotFound(TableNotFound),
    BadSQL(BadSQL),

}
#[derive(Debug, PartialEq, Clone)]
pub struct TableNotFound {
    description: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct BadSQL {
    description: String,

}



impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::TableNotFound(e) => write!(f, "Table Not Found: {}", e.description),
            ParseError::BadSQL(e) => write!(f, "BadSQL Error: {}", e.description),
            _ => write!(f, "")
        }
    }
}


impl Error for ParseError {}

pub fn parse(
    sql: &str,
    ) -> Vec<Result<HashMap<std::string::String, ArrayData>, ParseError>> {
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
                println!("\n\ncreate_table name: {:?}", name);
                println!("\n\ncreate_table columns: {:?}", columns);
                println!("\n\ncreate_table query: {:?}", query);
                handle_create_table(name.to_string(), columns);
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
            } => handle_insert(table_name.to_string(), columns, source),
            _ => println!("SQL type not yet supported"),
        }
    }
    println!("returning from parse: {:?}", query_results);
    return query_results
    } else {
        let err = Err(ParseError::BadSQL(BadSQL {description: format!("Could not parse {}", sql)}));
        return vec![err]

    }
}

fn handle_insert(table_name: String, column_names: &Vec<Ident>, source_data: &Box<Query>) {
    // Create RecordBatch from existing data
    let path = format!("tables/{}.parquet", table_name);
    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();

    if let Some(reader_option) = reader.next() {
        println!("\nreader_option: {:?}", reader_option);
        if let Ok(record_batch_one) = reader_option {
            let schema = record_batch_one.schema();

            // let arrow_writer = ArrowWriter::try_new(file, schema.clone(), None);

            // Grab Schema from existing RecordBatch
            let record_batch_two = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![7, 8])),
                    Arc::new(StringArray::from(vec!["James", "Stevens"])),
                    Arc::new(StringArray::from(vec!["Anthropology", "German"])),
                ],
            );
            // source_data = &Box<Query> -> Query -> Body = Box<SetExpr> -> SetExpr -> Values -> rows
            // let body = *source_data.body.clone();
            // match body {
            //     SetExpr::Values(v) => {
            //         for row in v.rows {
            //             println!("row: {:?}", row);
            //             for (i, val) in row.iter().enumerate() {
            //                 println!("i: {:?}; val: {:?}", i, val.to_string());
            //             }
            //         }
            //     }
            //     _ => println!("did not find values"),
            // }

            // Create Second RecordBatch using existing Schema and source_data

            let concatenated_columns = record_batch_one
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let col1 = record_batch_one.column(i);
                    let col2 = record_batch_two.as_ref().unwrap().column(i);
                    arrow::compute::concat(&[col1, col2]).unwrap()
                })
                .collect::<Vec<_>>();

            let concatenated_batches =
                RecordBatch::try_new(schema.clone(), concatenated_columns).unwrap();
            println!("concatenated_batches: {:?}", concatenated_batches);

            let file_to_write = File::open(&path);
            println!("file_to_write: {:?}", file_to_write);

            if let Ok(f) = file_to_write {
                let mut writer =
                    ArrowWriter::try_new(f, concatenated_batches.schema(), None).unwrap();

                writer.write(&concatenated_batches).expect("Writing batch");
                writer.close().unwrap();
                println!("file written");
            } else {
                println!("file not written");
            }

            // Write concatenated_batches'
        }
    }
}

fn convert_sqlparserdatatype_to_arrowdatatype(sqlparserdatatype: &DataType) -> arrow_datatype {
    match sqlparserdatatype {
        sqlparser::ast::DataType::Varchar(_char_len) => arrow::datatypes::DataType::Utf8,
        sqlparser::ast::DataType::Int(_i) => arrow::datatypes::DataType::Int32,
        _ => arrow::datatypes::DataType::Utf8,
    }
}

pub fn handle_create_table(table_name: String, columns: &Vec<ColumnDef>) {
    let mut schema_fields: Vec<Field> = Vec::new();

    for column in columns {
        let name = column.name.to_string();
        let sqldatatype = &column.data_type;

        let datatype = convert_sqlparserdatatype_to_arrowdatatype(&sqldatatype);
        schema_fields.push(Field::new(name, datatype, false));
    }
    let schema = Schema::new(schema_fields);

    let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
    let file = File::create(format!("tables/{}.parquet", table_name)).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    let _res = writer.write(&batch);
    writer.close().unwrap();
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
        let path = format!("tables/{}.parquet", table_name);
        let mut return_table = HashMap::new();
        if let Ok(file) = File::open(path) {
            let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();

            let next_reader = reader.next();
            if let Some(record_batch_option) = next_reader {
                if let Ok(record_batch) = record_batch_option {
                    let schema = record_batch.schema();
                    println!("schema inside of get_table: {:?}", schema);
                    for col_name in columns {
                        let recordbatch_column = record_batch.column_by_name(&col_name);
                        return_table
                            .insert(col_name.to_string(), recordbatch_column.unwrap().to_data());
                    }
                }
            }
            // let record_batch = reader.next().unwrap().unwrap();
        println!("returning from get table:\n {:?}", return_table);
        return Ok(return_table)
        } else {
            let err =  Err(ParseError::TableNotFound(TableNotFound { description: (format!("Could not find {} in {}.", String::from(table_name), "tables"))}));
            println!("returning from get_table:\n {:?}", err);
            return err
        }
    }
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



#[cfg(test)]
mod tests { 
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
        assert_eq!(parse_res.len(), 1);
        assert!(parse_res[0].is_err());

        match &parse_res[0] {
            Err(parse_error) => match parse_error {
                ParseError::TableNotFound(err) => assert_eq!(err.description, format!("Could not find {} in {}.", table_name, "tables")),
           _ => panic!("Expected TableNotFound error."),
            },
            _ => panic!("Expected TableNotFound error."),
        }
    }
}
