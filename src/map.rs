//! Pathfinding glue

use std::collections::HashMap;

use super::db::Db;
use super::Id;
use pathfinding::directed::dijkstra::{dijkstra_all, dijkstra};

pub struct Map<'d> {
    db: &'d mut Db,
    to: Id,
    map: HashMap<Id, (Id, usize)>,
}

fn successors<'d>(db: &'d Db) -> impl Fn(&u64) -> Box<dyn Iterator<Item=(Id,usize)> + 'd> {
    |&to| {
        Box::new(db.links(to).unwrap().into_iter()
            .map(|id| (id, 1)))
    }
}

pub fn path(db: &Db, from: &str, to: &str) -> Option<Vec<String>> {
    let from = db.index(from)?;
    let to = db.index(to)?;  

    let path = dijkstra(&from, successors(db), |&x| x == to)?;
    path.0.iter().map(|&i| db.lookup(i)).collect()
}

impl<'d> Map<'d> {

    pub fn build(db: &'d mut Db, to: &str) -> Option<Self> {

        let to = db.index(to)?;

        let map = dijkstra_all(&to, successors(db));

        Some(Self { db, to, map })
    }

    pub fn find(&self, from: &str) -> Option<Vec<String>> {
        let mut path = vec![from.to_owned()];

        let mut from = self.db.index(from)?;

        while from != self.to {
            from = self.map.get(&from)?.0;
            path.push(self.db.lookup(from)?)            
        }

        Some(path)
    }

}

