//! Pathfinding glue

use std::collections::HashMap;

use super::sqlite::Db;
use super::Id;

pub struct Map<'d> {
    db: &'d Db,
    to: Id,
    map: HashMap<Id, Id>,  // map node to next hop
}

impl<'d> Map<'d> {

    pub fn build(db: &'d Db, to: &str) -> Option<Self> {

        let to = db.index(to)?;

        let mut map = HashMap::new();
        let mut current = vec![to];
        let mut next = vec![];

        while !current.is_empty() {

            for &c in &current {
                let links = db.links_to(to);
                map.extend(links.iter().map(|&f| (c,f)));
                next.extend(links);
            }
            std::mem::swap(&mut current, &mut next);
            next.clear();
        }

        Some(Self { db, to, map })
    }

    pub fn find<'a>(&self, from: &str) -> Option<Vec<String>> {
        let mut path = vec![from.to_owned()];
        let mut current = self.db.index(from)?;

        while current != self.to {
            current = *self.map.get(&current)?;
            path.push(self.db.lookup(current)?);
        }

        Some(path)
    }

    pub fn save(&self, path: &str) {
        todo!()
    }

    pub fn load(db: &Db, path: &str) -> Self {
        todo!()
    }

}

