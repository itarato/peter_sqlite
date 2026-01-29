use anyhow::Result;
use clap::Parser;
use log::info;
use std::{fs::File, io::Read};

use crate::{database::Database, query::Query, query_executor::QueryExecutor, reader::Reader};

mod btree_page_header;
mod cell;
mod common;
mod database;
mod database_header;
mod query;
mod query_executor;
mod reader;
mod record;
mod schema;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct ProgramArgs {
    db_file_name: String,
    command: String,
}

fn main() -> Result<()> {
    // unsafe { std::env::set_var("RUST_LOG", "debug") };
    pretty_env_logger::init();

    info!("Peter SQLite Start");

    let args = ProgramArgs::parse();
    let mut file = File::open(&args.db_file_name)?;
    let mut buffer = vec![];
    file.read_to_end(&mut buffer).unwrap();
    let reader = Reader::new(&buffer[..]);
    let db = Database::from(&reader).unwrap();

    match args.command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.header.page_size);
            println!("number of tables: {}", db.tables.len());
        }
        ".tables" => {
            println!("{}", db.table_names_sorted().join(" "));
        }
        other => {
            let query = Query::parse(other);
            QueryExecutor::execute_query(&query, &db, &reader);
        }
    }

    Ok(())
}
