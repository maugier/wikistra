use clap::{Parser, Subcommand, ValueEnum};

pub use Command::*;

pub fn parse() -> Args {
    Args::parse()
}

#[derive(Parser)]
pub struct Args {
    #[command(subcommand)]
    pub cmd: Command,

    /// Database path (default: ./<wikiname>-db.sq3)
    #[arg(short, long)]
    pub db_path: Option<String>,

    /// Name of the wiki to dump from Wikimedia archives
    #[arg(short, long, default_value="enwiki")]
    pub wikiname: String,
}

#[derive(PartialEq,Eq,Debug,ValueEnum,Clone,Copy)]
pub enum Table {
    /// Maps article names to article IDs
    Page,

    /// Maps redirected articles to the redirection target
    Redirect,

    /// IDs of articles related by a link
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
    /// Download dumps from the 
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