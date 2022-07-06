package com.xiaohongshu.sql.datagen;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class LookupJoindemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);


        String sourcesql = "create table datagen (\n" +
                " userid int ,\n" +
                " proctime as PROCTIME()\n" +
                " ) with(\n" +
                " 'connector'='datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100'\n" +
                ")";
        tableEnv.executeSql(sourcesql);

        String dimSql =  "CREATE TABLE dim_mysql (\n" +
                "  id int,\n" +
                "  name STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
//                "   'driver'='com.mysql.cj.jdbc.Driver'\n,"+
                "   'url' = 'jdbc:mysql://localhost:3306/data_test?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false',\n" +
                "   'table-name' = 'dim_mysql',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")";
        tableEnv.executeSql(dimSql);

        String querySql = "SELECT * FROM datagen\n" +
                "LEFT JOIN dim_mysql FOR SYSTEM_TIME AS OF datagen.proctime \n" +
                "ON datagen.userid = dim_mysql.id";
        Table table = tableEnv.sqlQuery(querySql);
        tableEnv.toAppendStream(table, Row.class).print();
        streamEnv.execute("lookup_join_demo");



    }
}

