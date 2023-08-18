//! SQLite backend
use rusqlite::{Connection, Error, Row};


use super::Id;

pub struct Db {
    inner: Connection,
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.inner.execute_batch("PRAGMA optimize;");
    }
}

impl Db {

    pub fn new(path: &str) -> Result<Self, Error> {
        let inner = Connection::open(path)?;
        inner.execute_batch("
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = 0;
            PRAGMA cache_size = 100000;
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
        ")?;
        Ok(Self { inner })
    }

    pub fn search(&mut self, regex: &str) -> Vec<(Id, String)> {

        self.inner.prepare_cached("SELECT id, title FROM page WHERE title LIKE ?1")
            .unwrap()
            .query((regex,))
            .unwrap()
            .mapped(|r| Ok((r.get(0)?, r.get(1)?)))
            .map(|r| r.unwrap())
            .collect()
    }

    pub fn clear(&mut self) {
        self.inner.execute("DELETE FROM page", ()).unwrap();
        self.inner.execute("DELETE FROM link", ()).unwrap();
    }

    /// Insert an article in the DB. This updates both the forward and the reverse map.
    pub fn add(&mut self, id: Id, name: String) -> Result<(), Error>{
        self.inner.prepare_cached("INSERT INTO page VALUES (?1,?2)")
            .unwrap()
            .execute((id, name))?;
        Ok(())
    }

    /// Gives a list of all articles linking to this one
    pub fn links(&self, to: Id) -> Vec<Id> {
        self.inner.prepare_cached("SELECT `from` FROM link WHERE `to` = ?1")
            .unwrap()
            .query((to,))
            .unwrap() 
            .mapped(|row: &Row| -> Result<Id, Error> { row.get(0) })
            .map(Result::unwrap)
            .collect()
    }

    pub fn add_named_link(&mut self, from: Id, to: &str) {

    }

    /// Adds a link from one article to another
    pub fn add_link(&mut self, link: (Id, Id)) -> Result<(), Error> {
        self.inner.prepare_cached("INSERT OR IGNORE INTO link VALUES (?1,?2)")?
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
        .ok()
    }

    /// Lookup the article title given its ID
    pub fn lookup(&self, id: Id) -> Option<String> {
        self.inner.query_row("SELECT title FROM page WHERE id = ?1", (id,), 
        |row| row.get(0))
        .ok()
    }

}


#[cfg(test)]
mod test {
    use super::*;

    fn open_clean_db() -> Db {
        let mut db = Db::new("/tmp/dummydb.sq3").unwrap();
        db.clear();
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


        let mut links: Vec<_> = db.links(2);
        links.sort();


        assert_eq!(&links, &[1,3]);

    }

}