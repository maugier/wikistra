//! Bidirectional Dijkstra for constant-cost maps
//! 

use std::collections::BTreeMap;

use thiserror::Error;

use crate::sqlite::Db;


// Merge-union between two sorted lists, returns the first element appearing in both lists.
// Input lists must be sorted or this function may fail to find matches.
fn merge<'a, T: Ord>(mut xs: &'a [T], mut ys: &[T]) -> Option<&'a T> {
    while xs.len() > 0 && ys.len() > 0 {
        match xs[0].cmp(&ys[0]) {
            std::cmp::Ordering::Less => xs = &xs[1..],
            std::cmp::Ordering::Equal => return Some(&xs[0]),
            std::cmp::Ordering::Greater => ys = &ys[1..],
        }
    }
    None
}

#[derive(Debug)]
struct Front<T> {
    edge: Vec<T>,
    map: BTreeMap<T, T>,
}

fn check_collision<T: Ord + Copy>(from: &mut Front<T>, to: &mut Front<T>) -> Option<Vec<T>> {
    let &k = merge(&from.edge, &to.edge)?;

    let mut p = k;
    let mut path = vec![k];
    loop {
        let p2 = *from.map.get(&p).expect("inconsistent Front state");
        if p == p2 { break }
        path.push(p2);
        p = p2;
    }

    path.reverse();

    let mut n = k;
    loop {
        let n2 = *to.map.get(&n).expect("inconsistent Front state");
        if n == n2 { break }
        path.push(n2);
        n = n2;
    }

    Some(path)
}

impl <T: Ord + Copy> Front<T> {
    fn new(root: T) -> Self {
        let edge = vec![root];
        let mut map = BTreeMap::new();
        map.insert(root, root);
        Front { edge, map }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn expand<F,L>(&mut self, mut links: F, tmp: &mut Vec<T>)
    where
        F: FnMut(&T) -> L,
        L: IntoIterator<Item = T>,
    {
        for old in &self.edge {
            for new in links(old) {
                self.map.entry(new).or_insert_with(|| {
                    tmp.push(new);
                    *old
                });
            }
        }
        tmp.sort();
        std::mem::swap(tmp, &mut self.edge);
        tmp.clear();
    }

}

pub fn bidi_dijkstra<T,F1,F2,L1,L2>(start: T, goal: T, mut links_from: F1, mut links_to: F2) -> Option<Vec<T>>
where
    T: Ord + Copy + std::fmt::Debug,
    F1: FnMut(&T) -> L1,
    F2: FnMut(&T) -> L2,
    L1: IntoIterator<Item=T>,
    L2: IntoIterator<Item=T>,
{

    let mut from = Front::new(start);
    let mut to = Front::new(goal);

    let mut tmp_edge = vec![];

    loop {

        if let Some(path) = check_collision(&mut from, &mut to) {
            break Some(path);
        }

        if from.len() <= to.len() {
            from.expand(&mut links_from, &mut tmp_edge);
        } else {
            to.expand(&mut links_to, &mut tmp_edge);
        }

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

    let links_from = |from: &u32| db.links_from(*from);
    let links_to = |to: &u32| db.links_to(*to);

    let path = bidi_dijkstra(from, to, links_from, links_to)
        .ok_or(PathError::NoPathFound)?;

    Ok(path.iter().map(|&i| db.lookup(i).unwrap_or("???".to_owned())).collect::<Vec<_>>())

}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sample_merge() {
        assert_eq!(merge(&[1,3,5,7], &[4,5,6,7]), Some(&5))
    }

    #[test]
    fn empty_merge() {
        assert_eq!(merge(&[1,3,5], &[2,4,6]), None)
    }

    fn try_path(edges: &[(i32, i32)], from: i32, to: i32) -> Option<Vec<i32>> {
        let links_from = |f: &i32| { let f = *f; edges.iter().filter(move |&(a,_)| *a == f).map(|(_,b)| b).copied() };
        let links_to = |t: &i32| { let t = *t; edges.iter().filter(move |&(_,b)| *b == t).map(|(a,_)| a).copied() };
        bidi_dijkstra(from, to, links_from, links_to)
    }

    #[test]
    fn sample_path() {
        let edges = [(1,2), (1,3), (2,3), (3,4), (4,5), (5,1), (5,2)];

        assert_eq!(try_path(&edges[..], 1, 5), Some(vec![1,3,4,5]))

    }

}