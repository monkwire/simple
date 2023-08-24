mod parser;

fn main() {
    println!("Hello from simple_db");
    parser::parse("SELECT a FROM foo where x IN (SELECT y FROM bar); CREATE TABLE baz(q int)");
}
