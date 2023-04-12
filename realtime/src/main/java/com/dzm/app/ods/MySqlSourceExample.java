package com.dzm.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlSourceExample {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.184.100")
                .port(3306)
                .databaseList("gmall_flink") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList(".*") // set captured table
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        DataStreamSource<String> mySQL_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");// use parallelism 1 for sink to keep message ordering
        mySQL_source.print();
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
