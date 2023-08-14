use std::{fs::File, io::{BufReader, BufRead, SeekFrom, BufWriter, stdin}, collections::{BTreeMap}};

use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use indicatif::{self, ProgressBar, ProgressStyle, ProgressState};
use color_eyre::{Result, eyre::eyre};


mod cli;
mod sql;
mod source;
mod map;
mod memory;

pub type Id = u64;

use memory::Db;
use cli::*;
use regex::{RegexBuilder};

fn main() -> Result<()> {

    color_eyre::install()?;
    let args = cli::parse();

    match args.cmd {
        Download => source::download()?,
        Index => build_index(&mut memory::Db::new())?, 
            
        Search { query } => {
                
            let (source, progress) = open_gz_with_progress("./titledb.cbor.gz")?;
            progress.set_message("Loading title database");
            let index: BTreeMap<String, Id> = serde_cbor::from_reader(source)?;
            progress.finish_and_clear();
            drop(progress);

            if let Some(query) = query {
                search_db(&query, &index)?
            } else {
                eprintln!("Enter one query per line.");
                for line in stdin().lines() {
                    let line = line?;
                    if line == "" { continue };
                    if let Err(e) = search_db(line.trim(), &index) {
                        eprintln!("{}", e)
                    }
                }
            }

        }

        Parse { table } => {
            parse_table(table)?
        }
        Path { start, end } => {
            let db = memory::Db::new();
            let path = map::path(&db, &start, &end)
                .ok_or(eyre!("invalid path"))?;

            println!("{}", path.join(" -> "));

        },
        Map { end } => {
            let mut db = memory::Db::new();
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
        .with_style(style);

    let compressed = BufReader::new(progress.wrap_read(file));
    let reader = BufReader::new(GzDecoder::new(compressed));

    Ok((reader, progress))
}

fn build_index(db: &mut Db) -> Result<()> {

    let (source, progress) = open_gz_with_progress("./enwiki-latest-page.sql.gz")?;
    progress.set_message("Building title index");

    let (mut count, mut good) = (0,0);

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};
        count += 1;

        let id = field()?.int()? as u64;
        let ns = field()?.int()?;
        if ns != 0 { continue }
        let title = field()?.string()?;

        db.add(id, title);
        good += 1;
    }

    progress.finish_with_message(format!("Processed {} titles, {} in main namespace.", count, good));
    drop(progress);

    eprint!("Saving...");
    let tdb = File::options().create(true).write(true).open("./titledb.cbor.gz")?;
    let compressed = GzEncoder::new(BufWriter::new( tdb), Compression::default());
    serde_cbor::to_writer(compressed, &db.titles())?;
    eprintln!("ok.");
    
    (good, count) = (0,0);

    let (source, progress) = open_gz_with_progress("./enwiki-latest-pagelinks.sql.gz")?;
    progress.set_message("Building link map");

    for line in sql::Loader::load(source)? {
        let mut line = line?.into_iter();
        let mut field = || { line.next().ok_or(eyre!("invalid tuple"))};

        count += 1;

        let from = field()?.int()? as u64;
        let namespace = field()?.int()?;
        if namespace != 0 { continue; }
        let title = field()?.string()?;
        let from_ns = field()?.int()?;
        if from_ns != 0 { continue; }

        let Some(to) = db.index(&title) else { 
            eprintln!("Warning: Title not found in index: {}", &title);
            continue
        };

        db.add_link((from, to));
        good += 1;

    }

    progress.finish_with_message(format!("Processed {} links, {} in main namespace", count, good));
    drop(progress);

    eprint!("Saving...");
    let tdb = File::options().create(true).write(true).open("./linkdb.cbor")?;
    serde_cbor::to_writer(tdb, &db.linkmap())?;
    eprintln!("ok.");

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

fn search_db(query: &str, index: &BTreeMap<String, Id>) -> Result<()> {
    let matcher = RegexBuilder::new(query)
    .case_insensitive(true)
    .build()?;

    for (name, id) in index {
        if matcher.is_match(&name) {
            println!("[{}] {}", id, name);
        }
    }
    Ok(())
}

/*
#[test]
fn test_parse() {
    parse_table(0).unwrap();
}
*/
