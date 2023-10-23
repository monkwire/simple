use arrow::datatypes::SchemaRef;
use arrow_array::types::ByteArrayType;
use arrow_array::ArrayRef;
use arrow_array::Int32Array;
use arrow_array::StringArray;
use arrow_schema::Schema;
use parquet::data_type::FixedLenByteArray;
use parquet::data_type::FixedLenByteArrayType;
use parquet::data_type::Int32Type;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::file::writer::TrackedWrite;
use parquet::schema::types::SchemaDescriptor;
use parquet::{file::properties::WriterProperties, schema::types::Type};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
};

pub fn insert(path: &str) -> Result<(), std::io::Error> {}
