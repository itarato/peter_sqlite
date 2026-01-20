use anyhow::{Result, bail};
use clap::Parser;
use log::info;
use std::fs::File;
use std::io::BufReader;

use crate::database::Database;

mod common;
mod database;

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
    let mut buf_reader = BufReader::new(file);
    let db = Database::from(&mut buf_reader).unwrap();

    match args.command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size);
        }
        other => bail!("Missing or invalid command passed: {}", other),
    }

    Ok(())
}
