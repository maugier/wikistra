
use sled;

type Id = u64;

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
    #[error("bad link format")]
    BadLinkFormat,
}

pub fn parse_link(s: &str) -> Option<(Id, Id)> {
    let (k,v) = s.split_once(':')?;
    Some((k.parse().ok()?, v.parse().ok()?))
}

impl Db {

    pub fn open(path: &str) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let id = db.open_tree("id")?;
        let name = db.open_tree("name")?;
        let link = db.open_tree("link")?;
        Ok(Self { db, id, name, link })
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

    pub fn add_link(&mut self, (from, to): (Id, Id)) -> Result<(), sled::Error> {
        let key = format!("{}:{}", from, to);
        self.link.insert(key, &[])?;
        Ok(())
    }

    pub fn links(&mut self) -> impl Iterator<Item = Result<(Id,Id), Error>> {
        self.link.into_iter()
            .map(|pair| {
                let (k,_v) = pair?;
                let s = std::str::from_utf8(k.as_ref())?;
                parse_link(s).ok_or(Error::BadLinkFormat)
            })
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
        let mut db = Db::open("./testdb").unwrap();
        db.clear().unwrap();
        db
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
        db.add_link((3,1)).unwrap();

        let lnks: Result<Vec<_>, _> = db.links().collect();
        let lnks = lnks.unwrap();

        assert_eq!(&lnks, &[(1,2), (2,3), (3,1)]);

    }

}