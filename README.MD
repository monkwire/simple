![Simple Logo](https://github.com/Monkwire3/simple/blob/main/simple_image.png)


# Intro #
Simple is a database framework with fast read and write operations that allows expensive processes to be delayed and scheduled. The Simple API uses SQL input strings to modify Parquet files to represent data. The Simple Parquet file editor API functions can be called directly, providing more control to the user. Simple is written in Rust using the Parquet and Arrow crates.


# Installation #
To try Simple yourself, clone the directory into your project files:
`git clone https://github.com/monkwire/simple.git`

To use the database, save the program files into the `src` folder of your project, then import the mods you want.

# Usage #
The most straightforward way to use Simple is with the parse function. `parse` expects a string of valid SQL queries separated by semicolons.  Alternatively, more control can be exercised by importing the program modules directly and calling the following functions:
- `parse` - The `parse` function is the main entry-point of the database. `parse` handles a `&str` of semicolon-separated SQL queries, then returns an array of `Results` for each query. If the `&str` cannot be parsed as valid SQL, `parse` will return an array with a single `ParseError`.
- `handle_select` - This function handles a single select statement, and returns a `Result` containing either a table-representation of the select statement or a `ParseError`.
- `create` - This function creates a new Parquet file with a given schema and an optional argument for rows to be added. The created file can be one of many representing a Parquet table. See the directory structure section for more information.
- `insert` - The insert function creates a new Parquet table file for a table folder that already exists. `insert` will use the schema from an existing table file in the destination folder. If the folder is empty (or non-existent), an error will be thrown.
- `merge` - The merge function loads all files within a table folder into memory, then creates a new Parquet file which contains all file data from that folder.


# Directory Structure #
Simple stores all table data in a `tables` folder. By default, this folder lives in `/src/`, but you can change this location if you edit your `tables_path`. Tables are stored in folders using the naming convention `{table_name}/{table_name}_{file_number}.parquet`.


# Obstacles #
One significant hiccup I experienced when building this application is that editing Parquet files is more complicated and error-prone than I had expected. Parquet files are quick to read and write as a result of their formatting, which places metadata in the header and footer of the file. However, this file structure makes editing files quite involved, and ultimately, impractical. To overcome this obstacle, I chose to store tables as folders that contain one or more Parquet files. When `handle_select` is run, all files belonging to the selected table are read and loaded into memory, then processed. When a create or insert query is performed, only one file is read; the first table file is read to ensure that the new file conforms to the same schema as the existing table files.

The implication to this design is that table folders become bloated when many inserts are performed, with each `insert` call requiring an additional syscall when that table is queried. The `merge` function can be used to minimize this problem. `merge` is a potentially expensive function call which consolidates all Parquet files in a folder. Merge calls should be run during off times.

# Dependencies #
Simple is written using only Rust. In order to use this application, you must be able to compile and run Rust.

# Planned Updates #
This database is very much a work in progress. Currently only very basic SELECT queries are handled. Future updates will expand on this functionality.

