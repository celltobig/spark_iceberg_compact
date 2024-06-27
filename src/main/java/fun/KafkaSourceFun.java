package fun;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class KafkaSourceFun extends RichParallelSourceFunction<String> {

    private boolean flag = true;
    private boolean is_compress = true;
    private String format = null;
    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;
    private KafkaConsumer<String, String> consumer = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // kafka param conf
        Configuration conf = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String bootstrapServers = conf.getString("properties.bootstrap.servers","0");
        String groupId = conf.getString("properties.group.id","test");
        String clientId = conf.getString("properties.client.id","0");
        String topic = conf.getString("topic","test");
        String commit = conf.getString("properties.enable.auto.commit","false");
        format = conf.getString("format", "");
        if ("0".equals(bootstrapServers) || "0".equals(clientId) || clientId == null || "".equals(clientId) || "".equals(format)) {
            flag = false;
            return;
        }

        // 打开mysql中的开关
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://10.8.130.120:3306/flink_web1?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8&useSSL=false", "root", "ABCplm123!@#");
        // 可以封装成变量传入进来
        String sql = "select status from f_iceberg_table where iceberg_db_table_name='" + clientId + "' limit 1;";
        ps = conn.prepareStatement(sql);

//        System.out.println("conf = " + conf.toString() );
//        'properties.auto.commit.interval.ms' = '1000',
//        'canal-json.ignore-parse-errors' = 'true',
//        'format' = 'canal-json',
//        'properties.bootstrap.servers' = '10.8.49.24:9092,10.8.49.25:9092,10.8.49.26:9092,10.8.49.65:9092,10.8.49.66:9092',
//        'connector' = 'kafka',
//        'topic' = 'cowell_order_order_base',
//        'scan.startup.mode' = 'group-offsets',
//        'properties.auto.offset.reset.strategy' = 'earliest',
//        'properties.group.id' = 'prod_flink_group_v2',
//        'properties.enable.auto.commit' = 'true'

        // kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, commit);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rs != null) rs.close();
        if (ps != null) ps.close();
        if (conn != null) conn.close();
    }

    @Override
    public void run(SourceContext<String> out) throws Exception {
        while (flag) {
            // 改为定时去查，不要每次都查询
            rs = ps.executeQuery();
            if (rs.next()) {
                Integer status = rs.getInt("status");
                is_compress = (status == 1) ? true : false;
            }
            // 如果不压缩，则向下游发送数据
            if (!is_compress) {
                // 消费kafka 数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null) {
                        // 如果cdc 是 canal-json 的则处理
                        if("canal-json".equals(format)){
                            JSONObject row = JSON.parseObject(record.value());
                            JSONArray data = row.getJSONArray("data");
                            for (Object item: data) {
                                JSONObject tmp = (JSONObject) JSON.toJSON(item);
                                out.collect(tmp.toJSONString());
                            }
                        }else if("json".equals(format)){
                            out.collect(record.value().toString());
                        }
                    }
                }
            }
        }
    }


    @Override
    public void cancel() {
        flag = false;
    }
}
