//! In-memory database

use std::{collections::{HashMap, HashSet, BTreeMap}, rc::Rc, borrow::Borrow};
use serde::{Serialize, ser::{SerializeStruct}};

use super::Id;

#[derive(Eq,PartialEq,Hash,Ord,PartialOrd)]
struct Rcs(Rc<String>);

impl Rcs {
    fn new(s: String) -> Self {
        Self(Rc::new(s))
    }

    fn clone(other: &Self) -> Self {
        Rcs(Rc::clone(&other.0))
    }
}

impl Borrow<str> for Rcs {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Serialize for Rcs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        (*self.0).serialize(serializer)
    }
}

#[derive(Serialize)]
pub struct Titles<'d>(&'d BTreeMap<Rcs, u64>);

#[derive(Serialize)]
pub struct Links<'d>(&'d HashMap<u64, HashSet<u64>>);

/// Sled-backed database handle
#[derive(Default)]
pub struct Db {
    /// Map numerical IDs to article names
    id: HashMap<Id, Rcs>,
    /// Map article names to numerical IDs
    name: BTreeMap<Rcs, Id>,
    /// Map link destination ID to a CBOR array of source IDs
    link: HashMap<Id, HashSet<Id>>,
}

impl Db {

    /// Wrap an existing sled handle
    pub fn new() -> Self {
        Self::default()
    }

    /* 
    /// Clear the entire database
    pub fn clear(&mut self) {
        self.id.clear();
        self.name.clear();
        self.link.clear();
    }
    */

    /// Insert an article in the DB. This updates both the forward and the reverse map.
    pub fn add(&mut self, id: Id, name: String) {
        let name = Rcs::new(name);
        self.id.insert(id, Rcs::clone(&name));
        self.name.insert(name, id);
    }

    /// Gives a list of all articles linking to this one
    pub fn links(&self, to: Id) -> impl Iterator<Item = Id> + '_ {
        self.link.get(&to)
            .into_iter()
            .flat_map(|h| h.iter().copied())
    }

    /// Adds a link from one article to another
    pub fn add_link(&mut self, (from, to): (Id, Id)) {
        self.link.entry(to)
            .or_default()
            .insert(from);
    }

    /// Retrieves the article ID for a given title
    pub fn index(&self, name: &str) -> Option<Id> {
        self.name.get(name).cloned()
    }

    /// Lookup the article title given its ID
    pub fn lookup(&self, id: Id) -> Option<&str> {
        self.id.get(&id).map(Borrow::borrow)
    }

    pub fn titles(&self) -> Titles<'_> {
        Titles(&self.name)
    }

    pub fn linkmap(&self) -> Links<'_> {
        Links(&self.link)
    }

}

impl Serialize for Db {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
            let mut db = s.serialize_struct("db", 2)?;
            db.serialize_field("name", &self.name)?;
            db.serialize_field("links", &self.link)?;
            db.end()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn open_clean_db() -> Db {
        Db::new()
    }

    #[test]
    fn sample_titles_data() {
        let mut db = open_clean_db();  
        db.add(0, "foo".into());
        db.add(1, "bar".into());
        db.add(65537, "baz".into());

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

        db.add_link((1,2));
        db.add_link((2,3));
        db.add_link((3,2));


        let mut links: Vec<_> = db.links(2).collect();
        links.sort();


        assert_eq!(&links, &[1,3]);

    }

}