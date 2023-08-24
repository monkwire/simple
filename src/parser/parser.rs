use sqlparser::ast::{visit_statements, Statement};
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

    let mut i = 1;
    for statement in &unparsed_statements {
        println!("\nstatement {}", i);
        println!("statements: {:?}", statement);
        i += 1;

        match statement {
            Statement::Query(query) => println!("found query"),
            _ => println!("found not query"),
        }
    }

    let mut statements = vec![];
    visit_statements(&unparsed_statements, |stmt| {
        statements.push(format!("STATEMENT: {}", stmt));
        ControlFlow::<()>::Continue(())
    });

    for statement in statements {
        parse_statement(statement);
    }
}

fn traverse_select(query: &Box<sqlparser::ast::Query>) {}
