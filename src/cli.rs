use clap::{Parser, Subcommand};

pub use Command::*;

pub fn parse() -> Args {
    Args::parse()
}

#[derive(Parser)]
pub struct Args {
    #[command(subcommand)]
    pub cmd: Command,
    #[arg(short, long, default_value="./db")]
    pub db_path: String,
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage source files
    Download,

    /// Parse sql files into CSV
    Parse { 
        /// Index of the table to parse
        table: usize
    },

    /// Build index
    Index,

    Search { query: String },

    /// Compute single path from start to end
    Path { start: String, end: String },

    /// Compute full map to end, read several starts from stdin
    Map { end: String }

}