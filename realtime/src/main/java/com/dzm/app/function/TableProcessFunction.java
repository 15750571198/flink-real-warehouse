package com.dzm.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dzm.bean.TableProcess;
import com.dzm.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    //处理广播流数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);


        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            createHbaseTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }
        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();

        broadcastState.put(key,tableProcess);

    }
    //建表语句 : create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
    private void createHbaseTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        String[] fields;
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null){
                sinkPk="id";
            }

            if (sinkExtend == null){
                sinkExtend="";
            }

            StringBuffer createTableSQL = new StringBuffer(" create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");


            fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                if (sinkPk.equals(fields[i])){
                    createTableSQL.append(fields[i]).append(" varchar primary key");
                }else {
                    createTableSQL.append(fields[i]).append(" varchar");
                }

                if (i<fields.length-1){
                    createTableSQL.append(",");
                }
            }

            createTableSQL.append(")").append(sinkExtend);

            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        }finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName")+'-'+value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        //2.过滤字段
        if (tableProcess != null){
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            //将输出表/主题信息写入Value
            value.put("sinkTable",tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                //HBase数据,写入侧输出流
                ctx.output(objectOutputTag,value);
            }else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                //Kafka数据,写入主流
                out.collect(value);
            }

        }else {
            System.out.println("该组合Key：" + key + "不存在！");
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }


}
