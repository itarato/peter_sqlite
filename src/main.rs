use anyhow::{Result, bail};
use clap::Parser;
use log::info;
use std::fs::File;

use crate::database::Database;

mod btree_page_header;
mod cell;
mod common;
mod database;
mod database_header;
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
    let file = File::open(&args.db_file_name)?;
    let db = Database::from(file).unwrap();

    match args.command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.header.page_size);
            println!("number of tables: {}", db.tables.len());
        }
        ".tables" => {
            println!("{}", db.table_names_sorted().join(" "));
        }
        other => bail!("Missing or invalid command passed: {}", other),
    }

    Ok(())
}
