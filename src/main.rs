use std::{fs::File, io::{BufReader, BufRead, SeekFrom, stdin}};

use flate2::bufread::GzDecoder;
use indicatif::{self, ProgressBar, ProgressStyle, ProgressState};
use color_eyre::{Result, eyre::eyre};


mod cli;
mod sql;
mod source;
mod sqlite;
mod path;

pub type Id = u32;

use sqlite::Db;
use cli::*;

fn db_path(wikiname: &str, path: &Option<String>) -> String {
    path.as_ref()
        .map(|p| p.clone())
        .unwrap_or_else(|| format!("./{}-db.sq3", wikiname))
}

fn main() -> Result<()> {

    color_eyre::install()?;
    let args = cli::parse();

    let db_path = db_path(&args.wikiname, &args.db_path);

    match args.cmd {
        Download => source::download(&args.wikiname)?,
        Index { mode } => {
            let mut db = Db::new(&db_path)?;
            if let Some(Table::Page) | None = mode { build_page_index(&mut db, &args.wikiname)?; }
            if let Some(Table::Redirect) | None = mode { build_redirect_index(&mut db, &args.wikiname)?; }
            if let Some(Table::Link) | None = mode { build_link_index(&mut db, &args.wikiname)?; }
        },    

        Search { query } => {
                
            let mut db = Db::new(&db_path)?;

            if let Some(query) = query {
                for (id, title, redirect) in &db.search(&query) {
                    if let Some(target) = redirect {
                        println!("[{id}] {title} -> {target}")
                    } else {
                        println!("[{id}] {title}")
                    }
                }
            } else {
                eprintln!("Enter one query per line.");
                for line in stdin().lines() {
                    let line = line?;
                    if line == "" { continue };
                    
                    for (id, title, redirect) in &db.search(&line) {
                        if let Some(target) = redirect {
                            println!("[{id}] {title} -> {target}")
                        } else {
                            println!("[{id}] {title}")
                        }
                    }

                }
            }

        }

        Parse { table } => {
            parse_table(&args.wikiname, table.into())?
        }
        Path { start, end } => {
            let db = sqlite::Db::new(&db_path)?;
            let path = db.path(&start, &end)?;

            println!("{}", path.join(" -> "));

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

fn build_page_index(db: &mut Db, wikiname: &str) -> Result<()> {

    let path = format!("./{}-latest-page.sql.gz", wikiname);

    let (source, progress) = open_gz_with_progress(&path)?;
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

fn build_link_index(db: &mut Db, wikiname: &str) -> Result<()> {
    
    let (mut count, mut good, mut skip, mut bad) = (0,0,0,0);
    let path = format!("./{}-latest-pagelinks.sql.gz", wikiname);

    let (source, progress) = open_gz_with_progress(&path)?;
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

fn build_redirect_index(db: &mut Db, wikiname: &str) -> Result<()> {

    let path = format!("./{}-latest-redirect.sql.gz", wikiname);

    let (source, progress) = open_gz_with_progress(&path)?;
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

fn parse_table(wikiname: &str, table: usize) -> Result<()> {

    let filename = source::files(wikiname).nth(table)
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
