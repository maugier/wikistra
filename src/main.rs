use std::error::Error;
use indicatif::{self, ProgressBar};

mod cli;
mod sql;
mod db;
mod source;
mod map;

use db::Db;
use cli::*;

fn main() -> Result<(), Box<dyn Error>> {

    let args = cli::parse();

    match args.cmd {
        Source { mode: Clean }    => source::clean(),
        Source { mode: Download } => source::download(),
        Index { mode } => {
            let mut db = db::Db::open(&args.db_path)?;
            match mode {
                Build => build_index(&mut db),
                Clear => Ok(db.clear()?),
            }
        }
        Path { start, end } => todo!(),
        Map { end } => todo!(),
    }

}

fn build_index(db: &mut Db) -> Result<(), Box<dyn Error>> {

    let progress = ProgressBar::new_spinner()
        .with_message("Loading page titles");

    for line in sql::Loader::load("./enwiki-latest-page.sql.gz")? {
        let line = line?;
        
        progress.tick();

        println!("{:?}", line);

        let id: u64 = line[0].parse()?;
        let title = line[2].strip_prefix("'").unwrap().strip_suffix("'").unwrap().to_owned();

        db.add(id, &*title)?;
    }

    drop(progress);
    eprintln!("[*] Loaded {} titles", db.len());
    
    let progress = ProgressBar::new_spinner()
        .with_message("Loading links");

    for line in sql::Loader::load("./enwiki-latest-pagelinks.sql.gz")? {
        let line = line.unwrap();

        let from: u64 = line[0].parse().unwrap();
        let namespace: u64 = line[1].parse().unwrap();
        let title = &*line[2];

        let name = format!("{}:{}", namespace, title);

        let Some(to) = db.index(&name) else { continue };

        db.add_link((from, to)).unwrap();

    }

    drop(progress);

    Ok(())
}
