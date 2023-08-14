//! Utilities for dowloading the mysql dumps

use color_eyre::{Result, eyre::eyre};
use indicatif::{ProgressBar, ProgressStyle, ProgressState};
use std::{fs::File, io::Seek, ops::RangeInclusive, os::unix::prelude::MetadataExt};
use ureq::{self, Response};

static NAMES: [&str; 3] = ["page", "pagelinks", "redirect"];

static URL_BASE: &str = "https://dumps.wikimedia.org/enwiki/latest";

pub fn files() -> impl Iterator<Item = String> {
    NAMES.iter()
        .map(|n| format!("enwiki-latest-{}.sql.gz", n))
}

pub fn urls() -> impl Iterator<Item = String> {
    NAMES.iter().map(|f| format!("{}/enwiki-latest-{}.sql.gz", URL_BASE, f))
}

/// A parsed HTTP Content-Range header
pub struct Resume<'s> {
    pub unit: &'s str,
    pub total: Option<u64>,
    pub range: Option<RangeInclusive<u64>>
}

/// Parse an HTTP Content-Range header if present in the request
fn should_resume(res: &Response) -> Result<Option<Resume<'_>>> {
    let Some(range) = res.header("Content-Range") else { return Ok(None) };
    eprintln!("Range is {}", range);
    let (unit, range) = range.split_once(|c: char| c.is_whitespace())
        .ok_or(eyre!("Could not parse Content-Range header: no space"))?;
    let (range, total) = range.split_once('/')
        .ok_or(eyre!("Could not parse Content-Range header: no slash"))?;
    let total = if total == "*" { None } else { Some(total.parse()?) };
    let range = if range == "*" { None } else {
        let (start, end) = range.split_once('-')
            .ok_or(eyre!("Could not parse Content-Range header: no dash"))?;
            Some(start.parse()? ..= end.parse()?)
    };

    Ok(Some(Resume { unit, total, range }))
}

pub fn is_fresh(agent: &ureq::Agent, url: &str, path: &str) -> Option<()> {
    let file = File::open(path)
        .ok()?;
    let local = file.metadata().ok()?
        .size();

    let remote = agent.head(url)
        .call()
        .ok()?
        .header("Content-Length")?
        .parse().ok()?;

    if local == remote { Some(()) } else { None }

}

/// Download the source files. Resuming supported.
pub fn download() -> Result<()> { 

    let style = ProgressStyle::with_template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})").unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| { 
            write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
        })
        .progress_chars("=> ");

    let agent = ureq::AgentBuilder::new()
        .build();
    for (url, path) in urls().zip(files()) {

        if is_fresh(&agent, &url, &path).is_some() {
            eprintln!("{} up to date.", path);
            continue;
        }

        let mut file = File::options()
            .create(true)
            .append(true)
            .open(&path)?;

        file.seek(std::io::SeekFrom::End(0))?;
        let resume = file.stream_position()?;

        eprintln!("Existing file is {}", resume);

        let response = agent.get(&url)
            .set("Range", &format!("bytes={}-", resume))
            .call()?;

        let pos = if let Some(Resume { range: Some(r), ..}) = should_resume(&response)? {
            *r.start()
        } else {
            0
        };

        eprintln!("Starting download at offset {}", pos);

        file.seek(std::io::SeekFrom::Start(pos))?;

        let length: Option<u64> = response.header("Content-Length").map(str::parse).transpose()?;

        let progress = length
            .map(|l| ProgressBar::new(l))
            .unwrap_or(ProgressBar::new_spinner())
            .with_message(format!("Downloading {}", &path))
            .with_style(style.clone());

        let mut source = progress.wrap_read(response.into_reader());
        std::io::copy(&mut source, &mut file)?;

        progress.finish_with_message("Done.")

    }
    Ok(())
}
