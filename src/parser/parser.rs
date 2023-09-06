use arrow::array::ArrayData;
use arrow::datatypes::DataType as arrow_datatype;
use arrow::datatypes::{Field, Schema};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use sqlparser::ast::{self, ColumnDef, DataType, SelectItem, Statement};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

pub fn parse(
    sql: &str,
) -> Vec<Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>>> {
    let statements = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    let mut tables = Vec::new();

    for statement in &statements {
        match statement {
            Statement::Query(query) => {
                if let ast::SetExpr::Select(sel) = &*query.body {
                    let table = handle_select(&sel);
                    tables.push(table);
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
            }
            Statement::Insert { .. } => {
                println!("");
            }

            _ => println!("SQL type not yet supported"),
        }
    }
    tables
}

fn convert_sqlparserdatatype_to_arrowdatatype(sqlparserdatatype: &DataType) -> arrow_datatype {
    match sqlparserdatatype {
        sqlparser::ast::DataType::Varchar(_vc) => arrow::datatypes::DataType::Utf8,
        sqlparser::ast::DataType::Int(_i) => arrow::datatypes::DataType::Int32,
        _ => arrow::datatypes::DataType::Utf8,
    }
}

fn handle_create_table(table_name: String, columns: &Vec<ColumnDef>) {
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
) -> Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>> {
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
) -> Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>> {
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
        }
        println!("returning from get table:\n {:?}", return_table);
        Ok(return_table)
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
