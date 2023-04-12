package com.dzm;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class SqlServerToFinkCDC {
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
        Properties pro = new Properties();
        pro.setProperty("debezium.snapshot.isolation.mode","snapshot");
        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("192.168.8.15")
                .port(1433)
                .database("benlai") // monitor sqlserver database
                .tableList("dbo.DO_Master") // monitor products table
                .username("blx_lsy")
                .password("5YtveTGa")
                .debeziumProperties(pro)
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        env
                .addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
