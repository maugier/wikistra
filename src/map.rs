//! Pathfinding glue

use std::collections::HashMap;

use super::sqlite::Db;
use super::Id;
use pathfinding::directed::dijkstra::{dijkstra_all, dijkstra};
use thiserror::Error;

pub struct Map<'d> {
    db: &'d Db,
    to: Id,
    map: HashMap<Id, (Id, usize)>,
}

fn successors<'d>(db: &'d Db) -> impl Fn(&Id) -> Box<dyn Iterator<Item=(Id,usize)> + 'd> {
    |&to| {
        Box::new(db.links(to).into_iter()
            .map(|id| (id, 1)))
    }
}

#[derive(Error,Debug)]
pub enum PathError {
    #[error("Unknown article: {0}")]
    UnknownTitle(String),
    #[error("No path found")]
    NoPathFound
}

pub fn path(db: &Db, from: &str, to: &str) -> Result<Vec<String>, PathError> {
    let from = db.index(from)
        .ok_or_else(|| PathError::UnknownTitle(from.to_owned()))?;
    let to = db.index(to)
        .ok_or_else(|| PathError::UnknownTitle(to.to_owned()))?;  

    let path = dijkstra(&from, successors(db), |&x| x == to)
        .ok_or(PathError::NoPathFound)?;
    Ok(path.0.iter().map(|&i| db.lookup(i).unwrap_or("???".to_owned())).collect::<Vec<_>>())

}

impl<'d> Map<'d> {

    pub fn build(db: &'d Db, to: &str) -> Option<Self> {

        let to = db.index(to)?;

        let map = dijkstra_all(&to, successors(db));

        Some(Self { db, to, map })
    }

    pub fn find<'a>(&self, from: &str) -> Option<Vec<String>> {
        let mut path = vec![from.to_owned()];

        let mut from = self.db.index(from)?;

        while from != self.to {
            from = self.map.get(&from)?.0;
            path.push(self.db.lookup(from)?)            
        }

        Some(path)
    }

}

