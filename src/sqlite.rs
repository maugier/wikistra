//! SQLite backend
use rusqlite::{Connection, Error, Row, OpenFlags};
use thiserror::Error;


use crate::path::bidi_dijkstra;

use super::Id;

pub struct Db {
    inner: Connection,
}

/*
impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.inner.execute_batch("PRAGMA optimize;");
    }
}
*/

#[derive(Error,Debug)]
pub enum PathError {
    #[error("Unknown article: {0}")]
    UnknownTitle(String),
    #[error("No path found")]
    NoPathFound
}

impl Db {

    pub fn new(path: &str) -> Result<Self, Error> {
        let mut fresh = false;
        let mut flags = OpenFlags::default();
        flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        let inner = Connection::open_with_flags(path, flags)
            .or_else(|_| {
                fresh = true;
                Connection::open(path)
            })?;

        inner.execute_batch("
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = 0;
            PRAGMA cache_size = 100000;
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
        ")?;
        let mut new = Self { inner };
        if fresh { new.initialize()? };
        Ok(new)
    }

    fn initialize(&mut self) -> Result<(), Error> {
        self.inner.execute_batch("
            CREATE TABLE page (id int(8) primary key, title text unique) without rowid;
            CREATE TABLE link(`to` int(8), `from` int(8), primary key (`to`, `from`)) without rowid;
            CREATE TABLE redirect (id int(8) primary key, title text) without rowid;
            CREATE TABLE redirect_link (`to` int(8), `from` int(8), primary key (`to`, `from`));
            CREATE INDEX link_reverse ON link(`from`);
            CREATE INDEX redirect_link_reverse ON redirect_link(`from`);
        ")
    }

    pub fn search(&mut self, regex: &str) -> Vec<(Id, String, Option<String>)> {

        self.inner.prepare_cached("SELECT page.id, page.title, redirect.title FROM page LEFT JOIN redirect ON page.id = redirect.id WHERE page.title LIKE ?1")
            .unwrap()
            .query((regex,))
            .unwrap()
            .mapped(|r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map(|r| r.unwrap())
            .collect()
    }

    /// Insert an article in the DB. This updates both the forward and the reverse map.
    pub fn add(&mut self, id: Id, name: String) -> Result<(), Error>{
        self.inner.prepare_cached("INSERT INTO page VALUES (?1,?2)")
            .unwrap()
            .execute((id, name))?;
        Ok(())
    }

    pub fn links_to(&self, to: Id) -> Vec<Id> {
        let query = "SELECT `from` FROM link WHERE `to` = ?1 UNION SELECT `from` FROM redirect_link WHERE `to` = ?1";
        self.links_query(query, to)
    }

    pub fn links_from(&self, from: Id) -> Vec<Id> {
        let query = "SELECT `to` FROM link WHERE `from` = ?1 UNION SELECT `to` FROM redirect_link WHERE `from` = ?1";
        self.links_query(query, from)
    }

    /// Gives a list of all articles linking to this one
    pub fn links_query(&self, query: &'static str, to: Id) -> Vec<Id> {
        self.inner.prepare_cached(query)
            .unwrap()
            .query((to,))
            .unwrap() 
            .mapped(|row: &Row| -> Result<Id, Error> { row.get(0) })
            .map(Result::unwrap)
            .collect()
    }

    /// Adds a link from one article to another
    pub fn add_link(&mut self, link: (Id, Id)) -> Result<(), Error> {
        self.inner.prepare_cached("INSERT OR IGNORE INTO link(`from`, `to`) VALUES (?1,?2)")?
            .execute(link)?;
        Ok(())
    }

    pub fn add_redirect(&mut self, from: Id, title: &str) -> Result<(), Error> {
        self.inner.prepare_cached("INSERT OR IGNORE INTO redirect VALUES (?1, ?2)")?
            .execute((from, title))?;
        Ok(())
    }

    /// Retrieves the article ID for a given title
    pub fn index(&self, name: &str) -> Option<Id> {
        self.inner.query_row("SELECT id FROM page WHERE title = ?1", (name,),
        |row| row.get(0))
        .ok().flatten()
    }

    /// Lookup the article title given its ID
    pub fn lookup(&self, id: Id) -> Option<String> {
        self.inner.query_row("SELECT title FROM page WHERE id = ?1", (id,), 
        |row| row.get(0))
        .ok()
    }

    pub fn path(&self, from: &str, to: &str) -> Result<Vec<String>, PathError> {
        let from = self.index(from)
            .ok_or_else(|| PathError::UnknownTitle(from.to_owned()))?;
        let to = self.index(to)
            .ok_or_else(|| PathError::UnknownTitle(to.to_owned()))?;  
    
        let links_from = |from: &u32| self.links_from(*from);
        let links_to = |to: &u32| self.links_to(*to);
    
        let path = bidi_dijkstra(from, to, links_from, links_to)
            .ok_or(PathError::NoPathFound)?;
    
        Ok(path.iter().map(|&i| self.lookup(i).unwrap_or("???".to_owned())).collect::<Vec<_>>())
    
    }

}


#[cfg(test)]
mod test {
    use super::*;

    fn open_clean_db() -> Db {
        let path = "file::memory:";
        let mut db = Db::new(path).unwrap();
        db.initialize().unwrap();
        db
    }

    #[test]
    fn sample_titles_data() {
        let mut db = open_clean_db();  
        db.add(0, "foo".into()).unwrap();
        db.add(1, "bar".into()).unwrap();
        db.add(65537, "baz".into()).unwrap();

        assert_eq!(db.index("baz"), Some(65537));
        assert_eq!(db.index("foo"), Some(0));
        assert_eq!(db.index("nope"), None);

        assert_eq!(db.lookup(1).as_deref(), Some("bar"));
        assert_eq!(db.lookup(2).as_deref(), None);
        assert_eq!(db.lookup(65537).as_deref(), Some("baz"));

    }

    #[test]
    fn sample_link_data() {
        let mut db = open_clean_db();
        db.add_link((1,2)).unwrap();
        db.add_link((2,3)).unwrap();
        db.add_link((3,2)).unwrap();


        let mut links: Vec<_> = db.links_to(2);
        links.sort();


        assert_eq!(&links, &[1,3]);

    }

    #[test]
    fn sample_reverse_link_data() {
        let mut db = open_clean_db();
        db.add_link((1,2)).unwrap();
        db.add_link((2,3)).unwrap();
        db.add_link((3,2)).unwrap();


        let links: Vec<_> = db.links_from(2);
        assert_eq!(&links, &[3]);

    }

}