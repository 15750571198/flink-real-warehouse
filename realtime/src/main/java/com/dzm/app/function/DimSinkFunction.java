package com.dzm.app.function;

import com.alibaba.fastjson.JSONObject;
import com.dzm.common.GmallConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    //value:{"sinkTable":"dim_base_trademark","database":"gmall-210325-flink","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}
    //SQL：upsert into db.tn(id,tm_name) values('...','...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement =null;
        //获取SQL语句
        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = execUpsertSql(sinkTable, after);
            System.out.println(upsertSql);

            preparedStatement = connection.prepareStatement(upsertSql);
            preparedStatement.executeUpdate();
            preparedStatement.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    //data:{"tm_name":"Atguigu","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String execUpsertSql(String sinkTable, JSONObject after) {
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();


        return "upsert into "+GmallConfig.HBASE_SCHEMA+"."+sinkTable+"("+
                StringUtils.join(keySet,",")+") values('"+
                StringUtils.join(values,"','")+"')";

    }
}
