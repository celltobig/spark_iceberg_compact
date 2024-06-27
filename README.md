# spark_iceberg_compact
Flink 入湖，spark 压缩合并 iceberg 小文件 方案


数据湖方案 (文档比较乱，随手笔记)


背景：
1.使用iceberg 作为 数据湖组件，目前看，公认为最佳选择，支持主流flink,spark，无缝衔接，如果是准实时流方案，Flink入湖，由于flink是checkpoint的方式进行定时入湖的方式
每次都会以新文件的方式写，如果checkpoint 时间设置短，造成短时间内小文件过多的问题，衡量利利弊，checkpoint设置在分钟级别比较合适。1 、3、5 分钟 相对合适。
2.Flink采集入湖，Spark小文件合并压缩，如果flink 提交快照时，与压缩任务提前的冲突，会导致压缩提交失败，Flink采集周期短，压缩时间长，这个问题会非常明显，几乎不可用，时间长了，最终雪崩。

结论分析:Flink写与spark写冲突，可以使用lock 解决，或者只保证同一个时间，有一个写的提交，才能使用整个任务正常

解决方案1: 开源Amoro 方案，加表属性的方式，异步压缩，优点：无侵入flink任务，缺点：压缩flink任务需要大资源，且一直占用，达不到重复利用的效果；amoro JVM 内存偏大，否则有的 更新量大的表，执行计划会失败，导致服务不稳定，需要人工介入

解决方案2：自定义flink kafka source 源，需要压缩的时候，关闭写入数据的开关，从而达到 flink写数据无提交，直到压缩任务完成，再把开发打开，优点：官方提供的spark 压缩api，效果好，资源复用，并发高，
用完即释放，缺点：手动开发flink kafka source源，作业串行

结论：完美解决小文件问题 和 查询性能问题，spark的方式，内存可能会耗的多一些，自行测试
     flinkSql kafka 采用的是自动提交offset,这样如果任务 异常退出，可能会导致丢数，正常checkpoint 不会  
     如果 发现丢数，可以手动重置kafka offset，再跑一会数据，达到最终一致性

查询当前快照id 和时间

1.如果快照时间过于30分钟无更新，可以进行压缩任务
2.定时进行压缩任务，阻塞快照提交

目标：实现自定义kafka数据源，通过mysql配置开关来决定kafka数据向下游发送
压缩是使用 spark session 任务 进行压缩 执行 spark call
https://iceberg.apache.org/docs/nightly/spark-procedures/

使用环境及组件版本
flink: 1.18.1   https://flink.apache.org/
spark: 3.4.3    https://spark.apache.org/
iceberg: 1.5.0  https://iceberg.apache.org/


流程1：在Flink streaming 环境中 添加 addSource(new KafkaSourceFun())
     KafkaSourceFun 参考 src/main/java/fun/KafkaSourceFun 
      其他都是flinkSQL streaming 正常的开发流程，只修改 addSource 
流程2：执行 flinkSQL 入湖，参考 doc/flink.sql
流程3：运行 spark Session 定时期执行 spark call 
      参考 src/main/java/task/SparkSqlSumit
流程4：启动脚本
      参考 dock/spark.sh



参考明细：
-- 由于flink写的文件 ，基本上都是小文件 ，最好设置一个 target-file-size-bytes 参数，控制一下写入文件大小
107374182400 -- 100G
1048576      -- 1M
16777216     --16M
536870912  -- 512M
16777216   -- 16M
-- 压缩前把写表开关打开，以达到lock的目的
-- 正常执行压缩
CALL spark_catalog.system.rewrite_data_files(
table=>'dl_test.ods_cowell_order_order_base_stream_v2',
options=>map(
'max-concurrent-file-group-rewrites','200',
'target-file-size-bytes','134217728',
'min-input-files','5'
)
)
;
-- 指定最近的分区进行压缩
CALL spark_catalog.system.rewrite_data_files(
table=>'dl_test.ods_cowell_order_order_base_stream_v2',
options=>map(
'max-concurrent-file-group-rewrites','200',
'target-file-size-bytes','134217728',
'min-input-files','5'
),
where=>'dt>="20240620"'
);

-- 压缩 pos_del file  
-- 只能 压缩 pos-del file ,eq-del file 压缩合并不了
CALL spark_catalog.system.rewrite_position_delete_files(
table=>'dl_test.ods_cowell_order_order_base_stream_v2',
options=>map(
'max-concurrent-file-group-rewrites','200',
'target-file-size-bytes','134217728',
'min-input-files','2'
)
)
;

-- 每天保证一次全量重写压缩,其他控制参数设置无效
-- 可以把 eq-delete  和 pos-delete 文件 全部重写掉 
CALL spark_catalog.system.rewrite_data_files(
table=>'dl_test.ods_cowell_order_order_base_stream_v2',
options=>map(
'max-concurrent-file-group-rewrites','200',
'rewrite-all','true'
)
);


-- 全量压缩 pos_del file
CALL spark_catalog.system.rewrite_position_delete_files(
table=>'dl_test.ods_cowell_order_order_base_stream_v2',
options=>map(
'max-concurrent-file-group-rewrites','200',
'rewrite-all','true'
)
)
;


-- 本地启动 spark yarn session 环境， 手动执行 spark sql 
/data/appdata/spark-3.4.3-bin-without-hadoop/bin/spark-sql \
--conf spark.sql.debug.maxToStringFields=1000 \
--conf spark.rpc.message.maxSize=1024 \
--conf spark.sql.legacy.timeParserPolicy=LEGACY \
--conf spark.sql.storeAssignmentPolicy=ANSI \
--name lz-spark-sql-test --master yarn --deploy-mode client \
--driver-memory 4G \
--num-executors 4 --executor-memory 8G --executor-cores 4  \
--queue root.default



打个广告吧
兼职做：
1、前后端应用系统开发，实现高并发 高性能 高可用 。
2、大数据领域 spark flink hadoop体系 流计算、批处理 、etl 。
3、技术支持及解决方案 。

微信：celltobigs

