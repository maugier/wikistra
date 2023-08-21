use std::{fs::File, io::{BufReader, BufRead, SeekFrom, stdin}, collections::{BTreeMap}};

use flate2::{bufread::GzDecoder};
use indicatif::{self, ProgressBar, ProgressStyle, ProgressState};
use color_eyre::{Result, eyre::eyre};


mod cli;
mod sql;
mod source;
mod map;
mod memory;
mod sqlite;

pub type Id = u32;

use sqlite::Db;
use cli::*;
use regex::{RegexBuilder};

static DEFAULT_DB_PATH: &str = "./db.sq3";

fn main() -> Result<()> {

    color_eyre::install()?;
    let args = cli::parse();

    match args.cmd {
        Download => source::download()?,
        Index { mode } => {
            let mut db = Db::new(DEFAULT_DB_PATH)?;
            if let Some(Table::Page) | None = mode { build_page_index(&mut db)?; }
            if let Some(Table::Redirect) | None = mode { build_redirect_index(&mut db)?; }
            if let Some(Table::Link) | None = mode { build_link_index(&mut db)?; }
        },    

        Search { query } => {
                
            let mut db = Db::new(DEFAULT_DB_PATH)?;

            if let Some(query) = query {
                for (id, title) in &db.search(&query) {
                    println!("[{}] {}", id, &**title)
                }
            } else {
                eprintln!("Enter one query per line.");
                for line in stdin().lines() {
                    let line = line?;
                    if line == "" { continue };
                    
                    for (id, title) in &db.search(&line) {
                        println!("[{}] {}", id, &**title)
                    }

                }
            }

        }

        Parse { table } => {
            parse_table(table.into())?
        }
        Path { start, end } => {
            let db = sqlite::Db::new(DEFAULT_DB_PATH)?;
            let path = map::path(&db, &start, &end)?;

            println!("{}", path.join(" -> "));

        },
        Map { end } => {
            let db = sqlite::Db::new(DEFAULT_DB_PATH)?;
            let map = map::Map::build(&db, &end)
                .ok_or(eyre!("destination does not exist"))?;

            for start in std::io::stdin().lines() {
                let start = start?;
                if let Some(path) = map.find(&start) {
                    println!("{}", path.join(" -> "))
                } else {
                    println!("NO PATH {} -> {}", &start, &end)
                }
            }
        },
    }
    Ok(())
}

trait SeekLength: std::io::Seek {
    fn stream_length(&mut self) -> Result<u64, std::io::Error> {
        let old = self.seek(SeekFrom::Current(0))?;
        let pos = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(old))?;
        Ok(pos)
    }
}
impl <T: std::io::Seek> SeekLength for T {}

fn open_gz_with_progress(path: &str) -> Result<(impl BufRead, ProgressBar), std::io::Error> {

    let mut file = File::open(path)?;
    let length: Option<u64> = file.stream_length().ok();

    let style = ProgressStyle::with_template("[{elapsed_precise}] {msg} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})").unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| { 
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("=> ");

    let progress = length
        .map(|l| ProgressBar::new(l))
        .unwrap_or(ProgressBar::new_spinner())
        .with_style(style);

    let compressed = BufReader::new(progress.wrap_read(file));
    let reader = BufReader::new(GzDecoder::new(compressed));

    Ok((reader, progress))
}

fn build_page_index(db: &mut Db) -> Result<()> {

    let (source, progress) = open_gz_with_progress("./enwiki-latest-page.sql.gz")?;
    progress.set_message("Building title index");

    let (mut count, mut good) = (0,0);

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};
        count += 1;

        let id = field()?.int()? as Id;
        let ns = field()?.int()?;
        if ns != 0 { continue }
        let title = field()?.string()?;

        db.add(id, title)?;
        good += 1;
    }

    progress.finish_with_message(format!("Processed {} titles, {} in main namespace.", count, good));
    Ok(())
}

fn build_link_index(db: &mut Db) -> Result<()> {
    
    let (mut count, mut good, mut skip, mut bad) = (0,0,0,0);

    let (source, progress) = open_gz_with_progress("./enwiki-latest-pagelinks.sql.gz")?;
    progress.set_message("Building link map");

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};

        count += 1;

        let from = field()?.int()? as Id;
        let namespace = field()?.int()?;
        if namespace != 0 { skip += 1; continue; }
        let title = field()?.string()?;
        let from_ns = field()?.int()?;
        if from_ns != 0 { skip += 1; continue; }

        let Some(to) = db.index(&title) else {
            bad += 1;
            if bad < 1000 {
                eprintln!("Warning: Title not found in index: {}", &title);
            } else if bad == 1000 {
                eprintln!("Too many bad articles, skipping report");
            }
            continue
        };

        db.add_link((from, to))?;
        good += 1;

    }

    progress.finish_with_message(format!("Processed {} links ({} good, {} wrong namespace, {} missing from index)", count, good, skip, bad));
    drop(progress);

    Ok(())
}

fn build_redirect_index(db: &mut Db) -> Result<()> {

    let (source, progress) = open_gz_with_progress("./enwiki-latest-redirect.sql.gz")?;
    progress.set_message("Building redirect index");

    let (mut count, mut good) = (0,0);

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};
        count += 1;

        let id = field()?.int()? as Id;
        let ns = field()?.int()?;
        if ns != 0 { continue }
        let title = field()?.string()?;

        db.add_redirect(id, &title)?;
        good += 1;
    }

    progress.finish_with_message(format!("Processed {} titles, {} in main namespace.", count, good));
    Ok(())
}

fn parse_table(table: usize) -> Result<()> {

    let filename = source::files().nth(table)
        .ok_or(eyre!("No such table"))?;

    for row in sql::Loader::load_gz_file(&filename)? {
        let row = row?;
        println!("{:?}", row);
    }

    Ok(())
}

/*
#[test]
fn test_parse() {
    parse_table(0).unwrap();
}
*/
