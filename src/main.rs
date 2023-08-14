use std::{fs::File, io::{BufReader, BufRead, SeekFrom}};

use flate2::bufread::GzDecoder;
use indicatif::{self, ProgressBar, ProgressStyle, ProgressState};
use color_eyre::{Result, eyre::eyre};


mod cli;
mod sql;
mod db;
mod source;
mod map;

pub type Id = u64;

use db::Db;
use cli::*;

fn main() -> Result<()> {

    color_eyre::install()?;
    let args = cli::parse();

    match args.cmd {
        Source { mode: Clean }    => source::clean()?,
        Source { mode: Download } => source::download()?,
        Index { mode } => {
            let mut db = db::Db::open(&args.db_path)?;
            match mode {
                Build => build_index(&mut db)?,
                Clear => db.clear()?,
            }
        }
        Parse { table } => {
            parse_table(table)?
        }
        Path { start, end } => {
            let db = db::Db::open(&args.db_path)?;
            let path = map::path(&db, &start, &end)
                .ok_or(eyre!("invalid path"))?;

            println!("{}", path.join(" -> "));

        },
        Map { end } => {
            let mut db = db::Db::open(&args.db_path)?;
            let map = map::Map::build(&mut db, &end)
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
        .with_message(format!("Downloading {}", &path))
        .with_style(style);

    let compressed = BufReader::new(progress.wrap_read(file));
    let reader = BufReader::new(GzDecoder::new(compressed));

    Ok((reader, progress))
}

fn build_index(db: &mut Db) -> Result<()> {

    let (source, progress) = open_gz_with_progress("./enwiki-latest-page.sql.gz")?;
    progress.set_message("Building title index");

    db.clear()?;

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};

        let id = field()?.int()? as u64;
        let ns = field()?.int()?;
        if ns != 0 { continue }
        let title = field()?.string()?;

        db.add(id, &title)?;
    }

    progress.finish_with_message(format!("Loaded {} titles.", db.len()));
    drop(progress);
    
    let (source, progress) = open_gz_with_progress("./enwiki-latest-pagelinks.sql.gz")?;

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};

        let from = field()?.int()? as u64;
        let namespace = field()?.int()?;
        if namespace != 0 { continue; }
        let title = field()?.string()?;
        let from_ns = field()?.int()?;
        if from_ns != 0 { continue; }

        let Some(to) = db.index(&title) else { continue };

        db.add_link((from, to))?;

    }

    progress.finish_with_message(format!("Loaded {} links.", db.link_count()));
    drop(progress);

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

#[test]
fn test_parse() {
    parse_table(0).unwrap();
}
