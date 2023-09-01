use arrow::array::{self, ArrayData};
use arrow::datatypes::DataType as arrow_datatype;
use arrow::datatypes::{Field, Schema};
use arrow_array::{Int32Array, PrimitiveArray, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
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
                    let table_string = generate_table_string(table.unwrap());
                    // tables.push(table);
                }
            }
            Statement::CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                default_charset,
                collation,
                on_commit,
                on_cluster,
                order_by,
                strict,
            } => {
                println!("\n\ncreate_table name: {:?}", name);
                println!("\n\ncreate_table columns: {:?}", columns);
                println!("\n\ncreate_table query: {:?}", query);
                println!("\n\ncreate_table location: {:?}", location);
                handle_create_table(name.to_string(), columns);
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
                println!(
                    "Insert: into: {:?}, table_name: {:?}, columns: {:?}, after_columns: {:?}, source: {:?}",
                    into, table_name, columns, after_columns, source
                );
            }

            _ => println!("SQL type not yet supported"),
        }
    }
    tables
}

fn convert_sqlparserdatatype_to_arrowdatatype(sqlparserdatatype: &DataType) -> arrow_datatype {
    match sqlparserdatatype {
        sqlparser::ast::DataType::Varchar(vc) => arrow::datatypes::DataType::Utf8,
        sqlparser::ast::DataType::Int(i) => arrow::datatypes::DataType::Int32,
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
    println!("schema: {:?}", schema);

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

    let mut txt_cols: Vec<&String> = vec![];

    for column in columns {
        match column {
            SelectItem::UnnamedExpr(exp) => {
                if let ast::Expr::Identifier(ident) = exp {
                    txt_cols.push(&ident.value);
                }
            }
            SelectItem::Wildcard(_w) => {
                return get_table(
                    &select_statement.from[0].relation.to_string(),
                    &vec![],
                    true,
                )
            }
            _ => println!("found neither exp nor wildcard"),
        }
    }

    get_table(
        &select_statement.from[0].relation.to_string(),
        &txt_cols,
        false,
    )
}

fn get_table<'a>(
    table_name: &str,
    columns: &Vec<&'a String>,
    _wildcard: bool,
) -> Result<HashMap<std::string::String, ArrayData>, Box<dyn std::error::Error>> {
    let path = format!("tables/{}.parquet", table_name);
    let file = File::open(path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let record_batch = reader.next().unwrap().unwrap();

    let mut return_table = HashMap::new();

    for col_name in columns {
        let recordbatch_column = record_batch.column_by_name(col_name);
        return_table.insert(col_name.to_string(), recordbatch_column.unwrap().to_data());
    }

    Ok(return_table)
}

fn generate_table_string(arraydata: HashMap<String, ArrayData>) {
    println!("handle_array_data: {:?}", arraydata);

    let table_string = String::new();

    for (col_name, arr) in &arraydata {
        println!("arr datatype: {:?}", arr.data_type());
        match arr.data_type() {
            arrow_datatype::Utf8 => {
                let string_array = StringArray::from(arr.clone());
                println!("{:?}", string_array);
            }
            arrow_datatype::Int32 => {
                let int_array = Int32Array::from(arr.clone());
                println!("{:?}", int_array);
            }
            _ => println!("unsupported"),
        };
    }
}
