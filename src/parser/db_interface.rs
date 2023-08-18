use parquet::schema::parser::parse_message_type;

mod db_interface {
    struct Column {
        title: String,
        kind: Trait,
        required: bool,
    }


    // Writes table to db, then returns schema (Arc) if write was successfult
    pub fn write_table(columns: vec<Column>, title: String) {
        let file = fs::File::create(&Path::new(PATH)).unwrap();

        let mut message_type = format!("
            {} schema {{
                {}
        }}", title, columns);
    }
}
