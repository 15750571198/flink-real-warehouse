package com.dzm.app.function;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /**
     * 封装的数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "before":{"id":"","tm_name":""....},
     * "after":{"id":"","tm_name":""....},
     * "type":"c u d",
     * //"ts":156456135615
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector){
        //1.创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();
        Struct value = (Struct) sourceRecord.value();

        //2.获取库名&表名
        Struct source = value.getStruct("source");
        String database = source.get("db").toString();
        String tableName = source.get("table").toString();

        //3.获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                Object o = before.get(field);
                beforeJson.put(field.name(),o);
            }
        }
        //4.获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject arterJson = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                Object o = after.get(field);
                arterJson.put(field.name(),o);
            }
        }

        //5.获取操作类型  CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type="insert";
        }

        //6.将字段写入JSON对象
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",arterJson);
        result.put("type",type);

        //7.输出数据
        collector.collect(result.toJSONString());


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
