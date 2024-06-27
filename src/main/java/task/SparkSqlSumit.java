package task;

import com.mysql.cj.jdbc.MysqlDataSource;
import dao.IcebergTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

/**
 * spark sql jar
 */
public class SparkSqlSumit {

    /*
        mysqlUrl="jdbc:mysql://10.8.130.120:3306/flink_web1?characterEncoding=utf8&useSSL=false"
        mysqlUser=root
        mysqlPassword="ABCplm123!@#"
        mysqlDbTable=f_iceberg_table
        mysqlSql="select * from f_iceberg_table where rewrite=1"
        expireFiles=yes
        removeFiles=no
        rewritePosDelFiles=no
        rewriteFiles=no
        rewriteManifests=no
        useCache=false
     */

    private static Connection connection = null;
    private static List<IcebergTable> tableList = null;
    private static SparkSession spark = null;
    private static HiveCatalog catalog = new HiveCatalog();


    public static void main(String[] args) {

        String appName = "spark-iceberg-rewrite";
        Long sleepTime = 60 * 1000L; // 默认1分钟
        HashMap<String, String> argMap = new HashMap<String, String>();
        if (args.length > 0) {
            for (String arg : args) {
                String[] params = arg.trim().split("=");
                if (params.length == 2) {
                    argMap.put(params[0], params[1]);
                }
                if (arg.contains("mysqlUrl")) {
                    argMap.put("mysqlUrl", arg.trim().substring(9));
                }
                if (arg.contains("mysqlSql")) {
                    argMap.put("mysqlSql", arg.trim().substring(9));
                }
            }
        } else {
            return;
        }

        //默认全量压缩关闭
        argMap.put("fullRewrite", "no");
        if (argMap.containsKey("appName")) {
            appName = argMap.get("appName");
        }
        if (argMap.containsKey("sleepTime")) {
            sleepTime = Long.valueOf(argMap.get("sleepTime"));
        }

        String rewriteFiles = (argMap.containsKey("rewriteFiles")) ? argMap.get("rewriteFiles") : "no";
        String expireFiles = (argMap.containsKey("expireFiles")) ? argMap.get("expireFiles") : "no";
        String removeFiles = (argMap.containsKey("removeFiles")) ? argMap.get("removeFiles") : "no";
        String rewritePosDelFiles = (argMap.containsKey("rewritePosDelFiles")) ? argMap.get("rewritePosDelFiles") : "no";
        String rewriteManifests = (argMap.containsKey("rewriteManifests")) ? argMap.get("rewriteManifests") : "no";


        if (argMap.containsKey("mysqlUrl")) {
            connection = getConnection(argMap);
        }
        spark = getSparkSession(appName);
        setCatalog(spark);

        Long k = 0L;
        // 做成常驻进程，循环每次的压缩任务，保证每次 都进行压缩
        while (true) {
            k++;
            // 判断
            if (spark == null) {
                spark = getSparkSession(appName);
                setCatalog(spark);
                System.out.println("spark restart ...");
            }
            if (connection == null) {
                connection = getConnection(argMap);
                System.out.println("mysql restart ...");
            }

            // 定时 压缩，每天最后一次全量压缩后，就结束压缩
            Date currentDate = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("HHmmss");
            SimpleDateFormat dataTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time = dateFormat.format(currentDate);
            String dtime = dataTime.format(currentDate);
            tableList = getTableList(connection, argMap.get("mysqlSql"));
            if (tableList != null) {
                for (int i = 0; i < tableList.size(); i++) {
                    System.out.println("datetime = " + dtime);

                    String tableName = tableList.get(i).getIcebergDbTableName();
                    String mysqlDbTable = argMap.get("mysqlDbTable");
                    Long id = tableList.get(i).getId();
                    String fullRewriteDataFilesTime = tableList.get(i).getFullRewriteDataFilesTime().replace(":", "");
                    Long fullRewrite = tableList.get(i).getFullRewrite();

                    // 当处于全量压缩定时压缩的第一次时
                    if (fullRewrite == 0 && (Long.valueOf(time) >= Long.valueOf(fullRewriteDataFilesTime))) {
                        argMap.put("rewriteFiles", "no");
                        argMap.put("expireFiles", "no");
                        argMap.put("removeFiles", "no");
                        argMap.put("rewritePosDelFiles", "no");
                        argMap.put("rewriteManifests", "no");
                        argMap.put("fullRewrite", "yes");
                    } else {
                        argMap.put("rewriteFiles", rewriteFiles);
                        argMap.put("expireFiles", expireFiles);
                        argMap.put("removeFiles", removeFiles);
                        argMap.put("rewritePosDelFiles", rewritePosDelFiles);
                        argMap.put("rewriteManifests", rewriteManifests);
                        argMap.put("fullRewrite", "no");
                    }

                    // 修改 重写表的状态
                    String updateSql = "update " + mysqlDbTable + " set status=1,edit_time=now() where id=" + id;
                    if (connection != null) {
                        update(connection, updateSql);
                        // 也可以先等上几秒
                    }

                    // 当于设置时间长 要还原置为0
                    if (fullRewrite == 1 && (Long.valueOf(time) < Long.valueOf(fullRewriteDataFilesTime))) {
                        // 修改表状态
                        updateSql = "update " + mysqlDbTable + " set full_rewrite=0,edit_time=now() where id=" + id;
                        if (connection != null) {
                            update(connection, updateSql);
                        }
                    }

                    System.out.println("tableName = " + tableName);
                    String[] tmpTableName = tableName.split("\\.");
                    TableIdentifier name = TableIdentifier.of(tmpTableName[0], tmpTableName[1]);
                    Table table = catalog.loadTable(name);


                    // 压缩执行顺序  1压缩  2删除快照 3删孤文件 4合并
                    // 重写 pos del files
                    if ("yes".equals(argMap.get("rewritePosDelFiles"))) {
                        Long begin = System.currentTimeMillis();
                        String tmpSql = tableList.get(i).getRewritePositionDeleteFiles();
                        // 如果里面有 %s 的，要替换成 真实的表
                        if (tmpSql.contains("%s")) {
                            tmpSql = String.format(tmpSql, tableName);
                        }
                        System.out.println("rewrite_position_delete_files = " + tmpSql);
                        if (tmpSql != null && tmpSql.length() > 1) {
                            Dataset<Row> ds = spark.sql(tmpSql);
                            ds.show();

                            Long cost = (System.currentTimeMillis() - begin)/60000 ;
                            System.out.println("rewrite_position_delete_files cost time = " + cost + " m");
                        }
                    }

                    if ("yes".equals(argMap.get("rewriteFiles"))) {
                        Long begin = System.currentTimeMillis();
                        String tmpSql = tableList.get(i).getRewriteDataFiles();
                        // 如果里面有 %s 的，要替换成 真实的表
                        if (tmpSql.contains("%s")) {
                            tmpSql = String.format(tmpSql, tableName);
                        }

                        // where=>'dt>="20240620"'
                        if (tmpSql.contains("where=>'dt")) {
                            String regex = "%\\d%";
                            Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                            Matcher matcher = pattern.matcher(tmpSql);
                            int daysAgo = 0; // 你想要生成的天数
                            String group = "";
                            while (matcher.find()) {
                                group = matcher.group();
                                String group2 = group.replace("%", "");
                                System.out.println("Found the substring: " + group2);
                                daysAgo = Integer.valueOf(group2).intValue();
                            }
                            LocalDate date = LocalDate.now().minusDays(daysAgo);
                            String dataStr = date.toString().replace("-", "");
                            System.out.println("指定天数的日期是: " + dataStr);
                            tmpSql = tmpSql.replace(group, "%s");
                            tmpSql = String.format(tmpSql, dataStr);
                        }

                        System.out.println("rewrite_data_files = " + tmpSql);
                        if (tmpSql != null && tmpSql.length() > 1) {
                            Dataset<Row> ds = spark.sql(tmpSql);
                            ds.show();
                            Long cost = (System.currentTimeMillis() - begin)/60000 ;
                            System.out.println("rewrite_data_files cost time = " + cost + " m");
                        }
                    }

                    if ("yes".equals(argMap.get("fullRewrite"))) {
                        Long begin = System.currentTimeMillis();
                        String tmpSql = "CALL spark_catalog.system.rewrite_data_files(\n" +
                                "\ttable=>'%s',\n" +
                                "\toptions=>map(\n" +
                                "\t\t'max-concurrent-file-group-rewrites','200',\n" +
                                "\t\t'rewrite-job-order','files-desc',\n" +
                                "\t\t'rewrite-all','true'\n" +
                                "\t)\n" +
                                ")";
                        // 如果里面有 %s 的，要替换成 真实的表
                        if (tmpSql.contains("%s")) {
                            tmpSql = String.format(tmpSql, tableName);
                        }
                        System.out.println("rewrite_data_files = " + tmpSql);
                        if (tmpSql != null && tmpSql.length() > 1) {
                            Dataset<Row> ds = spark.sql(tmpSql);
                            ds.show();

                            Long cost = (System.currentTimeMillis() - begin)/60000 ;
                            System.out.println("fullRewrite rewrite_data_files cost time = " + cost + " m");
                        }

                        // 全量重写之后，删除所有的快照，保留最后一个
                        Long expireTime = System.currentTimeMillis() - (60 * 1000 * 1);
                        table.refresh();
                        System.out.println("expireTime = " + expireTime);
                        ExpireSnapshots.Result result = SparkActions
                                .get(spark)
                                .expireSnapshots(table)
                                .expireOlderThan(expireTime)
                                .execute();

                        System.out.println("ExpireSnapshots result = " + result.toString());

                        // 全量重写之后，删除所有的快照，保留最后一个
                        table.refresh();
                        DeleteOrphanFiles.Result result2 = SparkActions
                                .get(spark)
                                .deleteOrphanFiles(table)
                                .olderThan(expireTime)
                                .execute();
                        System.out.println("DeleteOrphanFiles result2 = " + result2.toString());

                        // 重写 meta
                        rewrite_manifests(spark, tableName, false);
                        // 修改表状态
                        updateSql = "update " + mysqlDbTable + " set full_rewrite=1,edit_time=now() where id=" + id;
                        if (connection != null) {
                            update(connection, updateSql);
                        }

                        Long cost = (System.currentTimeMillis() - begin)/60000 ;
                        System.out.println("fullRewrite cost time = " + cost + " m");
                    }


                    // 修改表状态
                    updateSql = "update " + mysqlDbTable + " set status=0,edit_time=now() where id=" + id;
                    if (connection != null) {
                        update(connection, updateSql);
                    }

                    if ("yes".equals(argMap.get("expireFiles"))) {
                        // 快照过期
                        Long begin = System.currentTimeMillis();
                        table.refresh();
                        Long expireSnapshot = tableList.get(i).getExpireSnapshot();
                        Long expireTime = System.currentTimeMillis() - (expireSnapshot * 1000 * 1);
                        long tsToExpire = expireTime;
                        ExpireSnapshots.Result result = SparkActions
                                .get(spark)
                                .expireSnapshots(table)
                                .expireOlderThan(tsToExpire)
                                .execute();

                        System.out.println("ExpireSnapshots result = " + result.toString());

                        Long cost = (System.currentTimeMillis() - begin)/60000 ;
                        System.out.println("ExpireSnapshots cost time = " + cost + " m");
                    }

                    if ("yes".equals(argMap.get("removeFiles"))) {
                        Long begin = System.currentTimeMillis();
                        table.refresh();
                        Long removeOrphan = tableList.get(i).getRemoveOrphan();
                        Long expireTime = System.currentTimeMillis() - (removeOrphan * 1000 * 1);
                        DeleteOrphanFiles.Result result = SparkActions
                                .get(spark)
                                .deleteOrphanFiles(table)
                                .olderThan(expireTime)
                                .execute();
                        int x=0;
                        while (result.orphanFileLocations().iterator().hasNext()){
                            String filepath = result.orphanFileLocations().iterator().next().toString();
                            System.out.println("DeleteOrphanFiles result filepath = " + filepath);
                            x++;
                        }
                        System.out.println("DeleteOrphanFiles result file num = " + x);

                        Long cost = (System.currentTimeMillis() - begin)/60000 ;
                        System.out.println("DeleteOrphanFiles cost time = " + cost + " m");
                    }

                    if ("yes".equals(argMap.get("rewriteManifests"))) {
                        Long begin = System.currentTimeMillis();
                        boolean useCache = argMap.containsKey("useCache") ? false : true;
                        rewrite_manifests(spark, tableName, useCache);

                        Long cost = (System.currentTimeMillis() - begin)/60000 ;
                        System.out.println("DeleteOrphanFiles cost time = " + cost + " m");
                    }


                    System.out.println("table =" + tableName + " rewrite over ");
                }
            }
            // spark.stop();
            System.out.println("exec end " + k);
            try {
                // 休息
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 重写manifests
     *
     * @param spark
     * @param tableName
     * @param use_cache
     */
    private static void rewrite_manifests(SparkSession spark, String tableName, boolean use_cache) {

        if (tableName == null) {
            return;
        }
        String tmpSql = "";
        if (use_cache) {
            tmpSql = "CALL spark_catalog.system.rewrite_manifests('" + tableName + "')";
        } else {
            tmpSql = "CALL spark_catalog.system.rewrite_manifests('" + tableName + "',false)";
        }
        System.out.println(tmpSql);
        try {
            Dataset<Row> ds = spark.sql(tmpSql);
            ds.show();
        }catch (Exception e){
            System.out.println("rewrite_manifests 异常 e=" + e.getMessage().toString());
        }
    }

    /**
     * 生产spark hadoop conf
     *
     * @return
     */
    private static SparkSession getSparkSession(String appName) {
//        System.setProperty("HADOOP_USER_NAME", "hive");
        SparkSession spark = SparkSession.builder()
//                .master("local[*]")
                .appName(appName)
                .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
                .config("spark.hadoop." + METASTOREURIS.varname, "thrift://10.8.49.115:9083")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("spark.executor.heartbeatInterval", "100000")
                .config("spark.network.timeoutInterval", "100000")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.defaultFS", "hdfs://nameservice2");
        spark.sparkContext().hadoopConfiguration().set("dfs.nameservices", "nameservice2");
        spark.sparkContext().hadoopConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        spark.sparkContext().hadoopConfiguration().set("dfs.ha.namenodes.nameservice2", "namenode37,namenode39");
        spark.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.nameservice2.namenode39", "bdp-datalake-namenode02-10-8-49-106:8020");
        spark.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.nameservice2.namenode37", "bdp-datalake-namenode01-10-8-49-105:8020");
        spark.sparkContext().hadoopConfiguration().set("dfs.client.failover.proxy.provider.nameservice2",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return spark;
    }

    private static void setCatalog(SparkSession spark) {
        catalog.setConf(spark.sparkContext().hadoopConfiguration());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", "/user/hive/warehouse");
        properties.put("uri", "thrift://10.8.49.114:9083,thrift://10.8.49.115:9083");
        catalog.initialize("hive", properties);
    }

    private static Connection getConnection(HashMap<String, String> argMap) {
        //1.0 创建数据源
        DataSource dataSource = new MysqlDataSource();
        ((MysqlDataSource) dataSource).setUrl(argMap.get("mysqlUrl"));
        ((MysqlDataSource) dataSource).setUser(argMap.get("mysqlUser"));
        ((MysqlDataSource) dataSource).setPassword(argMap.get("mysqlPassword"));
        //2.0 与数据库建立联系
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    private static List<IcebergTable> getTableList(Connection connection, String sql) {
        List<IcebergTable> list = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                IcebergTable table = new IcebergTable();
                table.setId(resultSet.getLong("id"));
                table.setIcebergDbTableName(resultSet.getString("iceberg_db_table_name"));
                table.setExpireSnapshot(resultSet.getLong("expire_snapshot"));
                table.setRemoveOrphan(resultSet.getLong("remove_orphan"));
                table.setRewriteDataFiles(resultSet.getString("rewrite_data_files"));
                table.setRewritePositionDeleteFiles(resultSet.getString("rewrite_position_delete_files"));
                table.setFullRewriteDataFilesTime(resultSet.getString("full_rewrite_data_files_time"));
                table.setFullRewrite(resultSet.getLong("full_rewrite"));
                table.setStatus(resultSet.getLong("status"));
                list.add(table);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    private static int update(Connection connection, String sql) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            int n = preparedStatement.executeUpdate();
            System.out.println(n);
            return n;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
