package com.dzm.app.ods;


import com.dzm.app.function.CustomerDeserialization;
import com.dzm.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class MySQLToFinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://master:8020/flink/ck"));
//        //开启ck
        env.enableCheckpointing(5000);
//        //设置精准一次模式
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 设置checkpoint的超时时间 即一次checkpoint必须在该时间内完成 不然就丢弃
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        // 设置最大并发checkpoint的数目
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        // 设置两次checkpoint之间的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("192.168.184.100")
                .databaseList("gmall_flink")
                .password("123456")
                .port(3306)
                .username("root")
                .tableList(".*")
                .startupOptions(StartupOptions.latest())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc");
        streamSource.print();

        //数据发送kafka
        String sinkTopic = "ods_base_db";
        //KafkaUtil.getKafkaProducer(sinkTopic)
        streamSource.addSink(KafkaUtil.getKafkaProducer(sinkTopic));


        env.execute();


    }
}
