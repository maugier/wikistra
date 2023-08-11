use std::error::Error;
use std::fs::File;
use ureq;

static NAMES: [&str; 2] = ["page", "pagelinks"];

static URL_BASE: &str = "https://dumps.wikimedia.org/enwiki/latest";

fn files() -> impl Iterator<Item = String> {
    NAMES.iter()
        .map(|n| format!("enwiki-latest-{}.sql.gz", n))
}

fn urls() -> impl Iterator<Item = String> {
    NAMES.iter().map(|f| format!("{}/enwiki-latest-{}.sql.gz", URL_BASE, f))
}

pub fn download() -> Result<(), Box<dyn Error>> { 
    let agent = ureq::AgentBuilder::new()
        .build();
    for (url, path) in urls().zip(files()) {

        let mut file = File::options()
            .write(true)
            .open(path)?;

        let mut source = agent.get(&url)
            .call()?
            .into_reader();

        std::io::copy(&mut source, &mut file)?;

    }
    Ok(())
}

pub fn clean() -> Result<(), Box<dyn Error>> {
    for file in files() {
        std::fs::remove_file(file)?;
    }
    Ok(())
 }
