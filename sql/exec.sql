-- user:root pwd:root
 use test;
 CREATE TABLE `t_books` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `title` varchar(100) DEFAULT NULL COMMENT '标题',
  `authors` varchar(100) DEFAULT NULL COMMENT '作者',
  `year` int(4) DEFAULT NULL COMMENT 'year',
  `create_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '日期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8