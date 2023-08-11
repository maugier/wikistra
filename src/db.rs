use sled;
use cbor;

use super::Id;

pub struct Db {
    db: sled::Db,
    id: sled::Tree,
    name: sled::Tree,
    link: sled::Tree,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sled")]
    Sled(#[from] sled::Error),
    #[error("encoding")]
    Encoding(#[from] std::str::Utf8Error),
    #[error("cbor")]
    CBOR(#[from] cbor::CborError),
}

fn encode(data: &[Id]) -> Result<Vec<u8>, Error> {
    let mut out = cbor::Encoder::from_memory();
    out.encode(data)?;
    Ok(out.into_bytes())
}

fn decode(mut blob: &[u8]) -> Result<Vec<Id>, Error> {
    Ok(cbor::Decoder::from_reader(&mut blob).decode().collect::<Result<_,_>>()?)
}

impl Db {

    pub fn from_sled(db: sled::Db) -> Result<Self, sled::Error> {
        let id = db.open_tree("id")?;
        let name = db.open_tree("name")?;
        let link = db.open_tree("link")?;
        Ok(Self { db, id, name, link })
    }

    pub fn open(path: &str) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Self::from_sled(db)
    }

    pub fn clear(&mut self) -> Result<(), sled::Error> {
        self.db.clear()
    }

    pub fn add(&mut self, id: Id, name: &str) -> Result<(), sled::Error> {
        let id = id.to_be_bytes();
        self.id.insert(id, name)?;
        self.name.insert(name, &id)?;
        Ok(())
    }

    pub fn links(&self, to: Id) -> Result<Vec<Id>, Error> {
        let Some(r) = self.link.get(to.to_be_bytes())? else { return Ok(vec![]) };
        Ok(decode(r.as_ref())?)
    }

    pub fn add_link(&mut self, (from, to): (Id, Id)) -> Result<(), Error> {
        let mut links = self.links(to)?;
        links.push(from);
        self.link.insert(to.to_be_bytes(), encode(&links)?)?;
        Ok(())
    }



    pub fn index(&self, name: &str) -> Option<Id> {
        let bytes = self.name.get(name).ok()??;
        let bytes: &[u8; 8] = bytes.as_ref().try_into().unwrap();
        Some(u64::from_be_bytes(*bytes))
    }

    pub fn lookup(&self, id: Id) -> Option<String> {
        let id = id.to_be_bytes();
        Some(std::str::from_utf8(self.id.get(&id).ok()??.as_ref()).ok()?.to_owned())
    }

    pub fn len(&self) -> usize {
        self.name.len()
    }

}

#[cfg(test)]
mod test {
    use super::*;

    fn open_clean_db() -> Db {
        let db = sled::Config::new()
            .temporary(true)
            .open().unwrap();
        Db::from_sled(db).unwrap()
    }

    #[test]
    fn sample_titles_data() {
        let mut db = open_clean_db();  
        db.add(0, "foo").unwrap();
        db.add(1, "bar").unwrap();
        db.add(65537, "baz").unwrap();

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

        assert_eq!(&db.links(2).unwrap(), &[1,3]);

    }

}