use sqlparser::ast::{self, Ident, SelectItem, Statement, Value};
use sqlparser::dialect::GenericDialect;
// use std::ops::ControlFlow;

// Top level parser
// Retuns Optional Table
// If table exists, returns table as requested in a select, or table with newly insterted data,
// or empty table

pub fn parse(sql: &str) {
    // Generate AST, identify statement type, then pass off function to appropriate parser.
    println!("hello from parse");

    let unparsed_statements =
        sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    for statement in &unparsed_statements {
        // println!("statements: {:?}\n", statement);

        match statement {
            Statement::Query(query) => match *query.body.clone() {
                ast::SetExpr::Select(sel) => {
                    println!("found select: {:?}", sel);
                    handle_select(&sel);
                }
                _ => println!("did not find select"),
            },
            _ => println!("query not found"),
        }
    }
}

fn handle_select(select_statement: &Box<sqlparser::ast::Select>) {
    println!("\nHello from handle_select");
    let columns = &select_statement.projection;

    let mut txt_cols: Vec<&String> = vec![];
    let tables = &select_statement.from;
    let table = tables[0].relation.to_string();

    for column in columns {
        match column {
            SelectItem::UnnamedExpr(exp) => {
                match exp {
                    ast::Expr::Identifier(ident) => {
                        println!("found ident: {:?}", ident.value);
                        let val: &String = &ident.value;
                        txt_cols.push(val);
                    }
                    _ => println!("did not find ident"),
                }
                println!("found unamed exp: {:?}", exp);
            }
            SelectItem::Wildcard(wild) => {
                println!("found wildcard: {}", wild);
            }
            _ => println!("found neither exp nor wildcard"),
        }
    }

    println!("columns: {:?}\n", columns);
    println!("txt_cols: {:?}\n", txt_cols);
    println!("table: {:?}\n ", table);

    get_table(&table, "./teachers.parquet", &txt_cols)
}

fn get_table(table_name: &str, path: &str, columns: &Vec<&String>) {
    println!("\nhello from get_table");
    println!(
        "table_name: {}, path: {}, columns: {:?}",
        table_name, path, columns
    );
}
