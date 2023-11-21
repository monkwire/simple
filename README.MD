[Simple Logo](https://github.com/Monkwire3/simple/blob/main/simple_image.png)


# Intro #
Simple is a database framework built to work with Parquet files. This database is ideal for databases that benefit from fast read and write operations, as expensive operations can be delayed and scheduled. The Simple API is designed to handle correct SQL input strings and modify Parquet files appropriately. For more control, the Parquet file editor modules can be exposed and the individual functions can be called directly. Simple is written in Rust using the Parquet and Arrow crates.

# Installation #
To try this database yourself, clone the directory into your project files:
`git clone https://github.com/Monkwire3/simple.git`

To use the database, save the program files into the `src` folder of your project, then import the mods you want.

# Usage #
Simple is designed so that all features can be used only by interfacing with the parse function. `parse` expects a string of valid SQL separated by semicolons. If you would like more control, the functions that interfaces with the database files can be exposed and called independently.

**Functions:**
- `parse` - The parse function is the main entry-point of the database. `parse` handles a `&str` of semicolon-separated SQL queries, then returns an array of `Results` for each query. If the `&str` cannot be parsed as valid SQL, `parse` will return an array with a single element: a `ParseError`.
- `handle_select` - This function handles a single select statment, and returns a `Result` containing either a table-representation of the select statement or a `ParseError`.
- `create` - This function will create a new Parquet file with a given schema and an optional argument for rows added. The `create` function is used to create a new Parquet file. The created file could be one of many representing a Parquet table. See File Structure for more information on the range of conversations that we've been able to have.
- `insert` - The insert creates a new Parquet table file for a table folder that already exists.
- `merge`- The merge function loads all files within a table folder into memory, then creates a new Parquet file which holds all file data into that folder.


# Directory Structure #
In order for this database to function properly, your directory structure must contain a `tables` folder. By default, this folder lives in `/src/`, but you can change this location if you edit your `tables_path`.

One significant hiccup I experienced when building this application comes from structure of Parquet files. Parquet files are quick to read and write as a result of their formatting, which places metadata in the header and footer of the file. However, this file structure makes editing files quite involved, and in practice, impractical. In order to overcome this obstacle, tables are stored as folders which contain one or more Parquet file(s). When `handle_select` is run, all files belonging to the selected table are read and loaded into memory, then processed. When a create or insert query is performed, only one file is read; the first table file is read to ensure that the new file conforms to the same schema as the existing table files.

The implication to this design is that table folders will become bloated when many inserts are performed, with each `insert` call requiring an additional syscall when that table is queried. This is where the `merge` command comes in. `merge` consolidates all Parquet files in a folder. Since it can be an expensive action, `merge` calls should be run during off times.

# Dependencies #
Simple was written using only Rust. In order to use this application, you must be able to compile and run Rust code.

# Planned Updates #
This database is very much a work in progress. Currently only very basic SELECT queries are handled. Future updates will expand on this functionality.

