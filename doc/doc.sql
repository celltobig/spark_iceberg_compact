
-- mysql 配置表 iceberg 待压缩的表
CREATE TABLE `f_iceberg_table` (
                                   `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
                                   `iceberg_db_table_name` varchar(100) DEFAULT NULL COMMENT '新生成湖中库表名',
                                   `expire_snapshot` int(11) DEFAULT '0' COMMENT '快照有效期',
                                   `remove_orphan` int(11) DEFAULT '0' COMMENT '孤文件有效期',
                                   `rewrite_data_files` varchar(1000) DEFAULT '' COMMENT '重写小文件',
                                   `rewrite_position_delete_files` varchar(1000) DEFAULT '0' COMMENT '重写posdel小文件',
                                   `full_rewrite_data_files_time` time DEFAULT NULL COMMENT '全部重写时间',
                                   `full_rewrite` int(11) DEFAULT '0' COMMENT '0没有完成，1已完成',
                                   `rewrite` int(11) DEFAULT '1' COMMENT '0不需要写1需要重写',
                                   `status` int(11) DEFAULT '1' COMMENT '0flink写入中 1spark重写中',
                                   `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                   `edit_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
                                   PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='iceberg 待压缩的表';


INSERT INTO `f_iceberg_table` (`id`, `iceberg_db_table_name`, `expire_snapshot`, `remove_orphan`, `rewrite_data_files`, `rewrite_position_delete_files`, `full_rewrite_data_files_time`, `full_rewrite`, `rewrite`, `status`, `create_time`, `edit_time`)
VALUES
    (40, 'dl_test.ods_cowell_order_order_base_stream_v2', 7200, 7200, 'CALL spark_catalog.system.rewrite_data_files(\n	table=>\'dl_test.ods_cowell_order_order_base_stream_v2\',\n	options=>map(\n		\'max-concurrent-file-group-rewrites\',\'200\',\n		\'target-file-size-bytes\',\'134217728\',\n		\'min-input-files\',\'5\'\n	),\n	where=>\'dt>=\"%7%\"\'\n)', 'CALL spark_catalog.system.rewrite_position_delete_files(\n	table=>\'dl_test.ods_cowell_order_order_base_stream_v2\',\n	options=>map(\n		\'max-concurrent-file-group-rewrites\',\'200\',\n		\'min-input-files\',\'5\'\n	)\n)', '10:00:00', 1, 1, 0, '2024-04-19 13:45:47', '2024-06-27 14:43:29');


