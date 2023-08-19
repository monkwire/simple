use parquet::schema::parser::parse_message_type;

    pub enum DataType {
        INT,
        BOOL,
        STRING,
    }


    pub struct Column {
        pub title: String,
        pub kind: DataType,
        pub required: bool,
    }


    // Writes table to db, then returns schema (Arc) if write was successfult
    pub fn write_table(columns: Vec<Column>, title: &str) {
        println!("write_table func")
        // let file = fs::File::create(&Path::new(PATH)).unwrap();

        // let mut message_type = format!("
        //     {} schema {{
        //         {}
        // }}", title, columns);
    }
