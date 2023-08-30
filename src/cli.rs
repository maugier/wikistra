use clap::{Parser, Subcommand, ValueEnum};

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

#[derive(PartialEq,Eq,Debug,ValueEnum,Clone,Copy)]
pub enum Table {
    Page,
    Redirect,
    Link,
}

impl Into<usize> for Table {
    fn into(self) -> usize {
        use Table::*;
        match self {
            Page => 0,
            Redirect => 1,
            Link => 2,
        }
    }
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage source files
    Download,

    /// Parse sql files into CSV
    Parse { 
        /// Index of the table to parse
        table: Table
    },

    /// Build index
    Index { mode: Option<Table> },

    /// Search the title database
    Search {
        /// A SQL pattern to match strings with. If absent, will work in interactive mode.
        query: Option<String>
    },

    /// Compute single path from start to end
    Path { start: String, end: String },

}