package com.dzm.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.dzm.util.KafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
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

        //TODO 2.读取Kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        kafkaDS.print();
        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.过滤数据  状态编程  只保留每个mid每天第一次登陆的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(key -> key.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dataState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("data-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig
                        .Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                dataState = getRuntimeContext().getState(stateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0) {
                    //取出状态数据
                    String lastDate = dataState.value();
                    //取出今天的日期
                    String ts = simpleDateFormat.format(value.getLong("ts"));
                    if (!ts.equals(lastDate)) {
                        dataState.update(ts);
                        return true;
                    }

                }
                return false;
            }
        });

        //TODO 5.将数据写入Kafka
        uvDS.map(JSONAware::toJSONString).addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 6.启动任务
        env.execute();

    }
}
