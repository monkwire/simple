use sqlparser::ast::visit_statements;
use sqlparser::dialect::GenericDialect;
use std::ops::ControlFlow;

// Top level parser
// Retuns Optional Table
// If table exists, returns table as requested in a select, or table with newly insterted data,
// or empty table
//
//
//

pub fn parse(sql: &str) {
    // Generate AST, identify statement type, then pass off function to appropriate parser.

    let unparsed_statements =
        sqlparser::parser::Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    let mut statements = vec![];
    visit_statements(&unparsed_statements, |stmt| {
        statements.push(format!("STATEMENT: {}", stmt));
        ControlFlow::<()>::Continue(())
    });

    for statement in statements {
        parse_statement(statement);
    }
}

pub fn parse_statement(statement: String) {
    let words: Vec<&str> = statement.split_whitespace().collect();

    for (i, word) in words.iter().enumerate() {
        println!("word {} :\n - {}", i, word);
    }

    match words[1] {
        "SELECT" => handle_select_statement(statement),
        _ => println!("not select"),
    }
}

pub fn handle_select_statement(statement: String) {
    println!("handle select statement func");
    // let relations: Vec<&str> =

    let expressions = sqlparser::parser::Parser::parse_sql(&GenericDialect, statement).unwrap();
}
