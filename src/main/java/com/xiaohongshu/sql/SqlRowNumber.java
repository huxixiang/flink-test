package com.xiaohongshu.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * https://blog.csdn.net/wangpei1949/article/details/105472473
 *
 * // Kafka中逐条输入如下数据
 * {"userID":"user_1","eventType":"click","eventTime":"2015-01-01 01:00:00","productID":"product_1"}
 * {"userID":"user_1","eventType":"click","eventTime":"2015-01-01 01:00:00","productID":"product_2"}
 * {"userID":"user_1","eventType":"click","eventTime":"2015-01-01 01:00:00","productID":"product_3"}
 *
 * // 输出
 * (true,user_1,click,2015-01-01 01:00:00,product_1)
 * (false,user_1,click,2015-01-01 01:00:00,product_1)
 * (true,user_1,click,2015-01-01 01:00:00,product_2)
 * (false,user_1,click,2015-01-01 01:00:00,product_2)
 * (true,user_1,click,2015-01-01 01:00:00,product_3)
 */
public class SqlRowNumber {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(streamEnv,settings);
        // Source DDL
        String sourceDDL = ""
                + "create table source_kafka "
                + "( "
                + "    userID String, "
                + "    eventType String, "
                + "    eventTime String, "
                + "    productID String, "
                + "    proctime AS PROCTIME() "
                + ") with ( "
                + "    'connector.type' = 'kafka', "
                + "    'connector.version' = 'universal', "
                + "    'connector.properties.bootstrap.servers' = 'localhost:9092', "
                + "    'connector.topic' = 'FLINK_TOPIC_TEST', "
                + "    'connector.properties.group.id' = 'flink-test', "
                + "    'connector.startup-mode' = 'latest-offset', "
                + "    'format.type' = 'json' "
                + ")";


        tableEnv.executeSql(sourceDDL);


        // Deduplication Query
        // 保留最后一条
        String execSQL = ""
                + "SELECT userID, eventType, eventTime, productID  ,proctime "
                + "FROM ( "
                + "  SELECT *, "
                + "     ROW_NUMBER() OVER (PARTITION BY userID, eventType, eventTime ORDER BY proctime DESC) AS rownum "
                + "  FROM source_kafka "
                + ") t "
                + "WHERE rownum = 1";
        Table resultTable = tableEnv.sqlQuery(execSQL);
        tableEnv.toRetractStream(resultTable, Row.class).print();


//        tableEnv.execute("");
        streamEnv.execute("");

    }
}
