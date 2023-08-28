use sqlparser::ast::{self, Statement};
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
        println!("statements: {:?}\n", statement);

        match statement {
            Statement::Query(query) => match *query.body.clone() {
                ast::SetExpr::Select(sel) => {
                    println!("found select: {:?}", sel);
                    handle_select(&sel);
                }
                _ => println!("did not find select"),
            },
            _ => println!("found not query"),
        }
    }
}

fn handle_select(select_statement: &Box<sqlparser::ast::Select>) {
    println!("Hello from handle_select");
    let columns = &select_statement.projection;
    let table = &select_statement.from;

    println!("columns: {:?}", columns);
    println!("table: {:?}", table);
}
