package com.dzm.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dzm.app.function.CustomerDeserialization;
import com.dzm.app.function.DimSinkFunction;
import com.dzm.app.function.TableProcessFunction;
import com.dzm.bean.TableProcess;
import com.dzm.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        //TODO 2.消费Kafka ods_base_db 主题数据创建流
        String sourceTopic="ods_base_db";
        String groupId= "base_db_app_1";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象并过滤(delete) 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出数据的操作类型
                        String type = value.getString("type");

                        return !"delete".equals(type);
                    }
                });
//
//
//        //TODO 4.使用FlinkCDC消费配置表并处理成         广播流
        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("192.168.184.100")
                .port(3306)
                .username("root")
                .password("123456")
                .deserializer(new CustomerDeserialization())
                .databaseList("gmall_flink_process")
                .tableList("gmall_flink_process.table_process")
                .build();

        DataStreamSource<String> tableProcessStrDS = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "process");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);
        tableProcessStrDS.print();
        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 6.分流  处理数据  广播流数据,主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){};
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //TODO 7.提取Kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        //TODO 8.将Kafka数据写入Kafka主题,将HBase数据写入Phoenix表
        kafka.print("Kafka>>>>>>>>");
        hbase.print("HBase>>>>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(KafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes());
            }
        }));


        //TODO 9.启动任务
        env.execute("BaseDBApp");

    }
}
