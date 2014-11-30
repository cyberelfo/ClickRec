CREATE TABLE `count_doc_hits` (
  `document_id` bigint(11) unsigned NOT NULL,
  `count` int(11) DEFAULT NULL,
  `url` varchar(1000) DEFAULT NULL,
  `filename` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`document_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `document` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `document_id` bigint(11) NOT NULL,
  `url` varchar(1000) DEFAULT NULL,
  `title` varchar(1000) DEFAULT NULL,
  `body` longtext,
  `publish_date` datetime DEFAULT NULL,
  `section` varchar(255) DEFAULT NULL,
  `modify_date` datetime DEFAULT NULL,
  `filename` varchar(50) DEFAULT NULL,
  `url_md5` char(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx01` (`document_id`,`filename`),
  KEY `idx02` (`url_md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `home_g1` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(1000) DEFAULT NULL,
  `datetime_crawl` datetime DEFAULT NULL,
  `url_md5` char(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx01` (`url_md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `path_sizes` (
  `path_size` int(11) unsigned NOT NULL,
  `num_users` int(11) DEFAULT NULL,
  `percent` float DEFAULT NULL,
  `filename` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`path_size`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stream` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `product_id` int(11) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `document_id` bigint(20) DEFAULT NULL,
  `provider_id` int(11) DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  `timestamp` varchar(13) DEFAULT NULL,
  `stream_datetime` datetime DEFAULT NULL,
  `filename` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stream_g1` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `product_id` int(11) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `document_id` bigint(20) DEFAULT NULL,
  `provider_id` int(11) DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  `timestamp` varchar(13) DEFAULT NULL,
  `stream_datetime` datetime DEFAULT NULL,
  `filename` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `transaction` (
  `user_id` bigint(11) NOT NULL,
  `documents` text,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `user_path_size` (
  `user_id` bigint(11) NOT NULL,
  `path_size` int(11) DEFAULT NULL,
  `filename` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `itemset` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `filename` varchar(50) DEFAULT NULL,
  `itemset_size` smallint(6) DEFAULT NULL,
  `itemset_id` smallint(6) DEFAULT NULL,
  `document_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;