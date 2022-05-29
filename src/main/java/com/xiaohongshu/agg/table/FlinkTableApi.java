package com.xiaohongshu.agg.table;

import com.xiaohongshu.agg.common.Event;
import com.xiaohongshu.common.flink.ExecutionEnvironmentClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * table api flink sql
 */
public class FlinkTableApi {
    public static void main(String[] args) throws Exception{
        //web prot:8082
        StreamExecutionEnvironment env = ExecutionEnvironmentClient.getStreamExecEnv();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        //table api的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        env.setParallelism(1);

        //1、读取数据源
        DataStreamSource<Event> source = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        // 2. 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(source);

        // 3. 用执行SQL 的方式提取数据
        Table resultTable1 = tableEnv.sqlQuery("select url, user from "+eventTable );

        // 4. 基于Table直接转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        // 5. 将表转换成数据流，打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        env.execute();


    }



}
