use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

pub use Command::*;
pub use SourceCommand::*;
pub use IndexCommand::*;

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
    Source { mode: SourceCommand },

    /// Manage database
    Index { mode: IndexCommand },

    /// Compute single path from start to end
    Path { start: String, end: String },

    /// Compute full map to end, read several starts from stdin
    Map { end: String }

}
#[derive(ValueEnum, Clone, Copy)]
pub enum SourceCommand {
    Download,
    Clean,
}

#[derive(ValueEnum, Clone, Copy)]
pub enum IndexCommand {
    Build,
    Clear,
}