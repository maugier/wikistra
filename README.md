# Wikistra - Finding shortest paths through wikipedia links.

This is a tool to win at the game of "Reach article B from
article A in the smallest number of link clicks".

## Todo 

 [X] Parse the dumps
 [X] Build the index
 [X] Lookup single paths
 [ ] Build a map cache
 [ ] Use the map in lookups

## Installation


Compile and install:

```
cargo install --path .
```

## Extract graph data

For english wikipedia, the data requires about 11GiB of disk space, plus
another free 11GiB while building the index.

Obtain a wikimedia database backup (or run `wikistra download` to download
the english dump from the official mirror). You need `.sql.gz` backups of the tables `page`,
`pagelinks`, and `redirect`.

Extract the MySQL dumps into a useable sqlite database with

```
wikistra index
```

The process is not fast, but it should be faster than restoring the backups into MySQL/MariaDB.


Once the index is built, you can delete the source `.sql.gz` backups.


## Usage

There are two usage modes: single path when both the start and destination pages are random,
and precomputed map when you will issue multiple queries from random start positions to a
single target known in advance.

### Single path

You can search for a path from A to B by running

```
wikistra path TITLE_A TITLE_B
```

Where `TITLE_A` and `TITLE_B` are the URL-safe titles of the article pages
(the last component of the article URL).

### Precomputed map

Run

```
wikistra map TITLE_END
```

with the chosen destination page. If this is the first 
