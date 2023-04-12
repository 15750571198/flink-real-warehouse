package com.dzm;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQLToFinkCDCtest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://master:8020/flink/ck"));
//        //开启ck
        env.enableCheckpointing(5000L);
//        //设置精准一次模式
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 设置checkpoint的超时时间 即一次checkpoint必须在该时间内完成 不然就丢弃
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        // 设置最大并发checkpoint的数目
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        // 设置两次checkpoint之间的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("10.80.32.67")
                .databaseList("wms2_db")
                .password("5YtveTGa.com")
                .port(3306)
                .username("blx_lsy")
                .tableList("wms2_db.transfer_master")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(),"mysql")
                .print().setParallelism(1);
        env.execute();


    }
}
