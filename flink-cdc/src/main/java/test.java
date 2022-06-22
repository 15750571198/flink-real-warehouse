import com.dzm.CustomerDeserialization;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000L);
        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("172.20.1.66")
                .database("testDB")
                .password("Password!")
                .username("sa")
                .port(1433)
                .tableList("dbo.student")
                .deserializer(new CustomerDeserialization())
                .build();

        env.addSource(sourceFunction)
                .print().setParallelism(1);
        env.execute();


    }

}
