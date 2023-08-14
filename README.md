# Wikistra - Dijkstra's algorithm on the Wikipedia graph

Features:

 - Downloads the Mysql DB dumps from wikipedia
 - Processes the dump with a custom SQL parser in O(1) space
 - Stores the article map in a Sled database


## Data sources

We use these mysql dumps provided by Wikipedia.

`page` contains page metadata. `page_id` is an opaque page identifier,
but the page can also be identified by a (namespace, title) tuple.

`pagelinks` contain the inter-wiki links. `pl_from` matches the `page_id` of the source page.
`pl_title` and `pl_namespace` match the `page_namespace` and `page_title` of the destination page.

```mysql
CREATE TABLE `pagelinks` (
  `pl_from` int(8) unsigned NOT NULL DEFAULT 0,
  `pl_namespace` int(11) NOT NULL DEFAULT 0,
  `pl_title` varbinary(255) NOT NULL DEFAULT '',
  `pl_from_namespace` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`pl_from`,`pl_namespace`,`pl_title`),
  KEY `pl_namespace` (`pl_namespace`,`pl_title`,`pl_from`),
  KEY `pl_backlinks_namespace` (`pl_from_namespace`,`pl_namespace`,`pl_title`,`pl_from`)
) ENGINE=InnoDB DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED;

CREATE TABLE `page` (
  `page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,
  `page_namespace` int(11) NOT NULL DEFAULT 0,
  `page_title` varbinary(255) NOT NULL DEFAULT '',
  `page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT 0,
  `page_is_new` tinyint(1) unsigned NOT NULL DEFAULT 0,
  `page_random` double unsigned NOT NULL DEFAULT 0,
  `page_touched` binary(14) NOT NULL,
  `page_links_updated` varbinary(14) DEFAULT NULL,
  `page_latest` int(8) unsigned NOT NULL DEFAULT 0,
  `page_len` int(8) unsigned NOT NULL DEFAULT 0,
  `page_content_model` varbinary(32) DEFAULT NULL,
  `page_lang` varbinary(35) DEFAULT NULL,
  PRIMARY KEY (`page_id`),
  UNIQUE KEY `page_name_title` (`page_namespace`,`page_title`),
  KEY `page_random` (`page_random`),
  KEY `page_len` (`page_len`),
  KEY `page_redirect_namespace_len` (`page_is_redirect`,`page_namespace`,`page_len`)
) ENGINE=InnoDB AUTO_INCREMENT=74185516 DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED;
```

## Loading

We need two datastructures:

 - The graph data (`page_id` to `page_id`)
 - The namespace:title interner


