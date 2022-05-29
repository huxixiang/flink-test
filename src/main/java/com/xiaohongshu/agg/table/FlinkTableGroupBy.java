package com.xiaohongshu.agg.table;

import com.xiaohongshu.agg.common.Event;
import com.xiaohongshu.common.flink.ExecutionEnvironmentClient;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableGroupBy {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = ExecutionEnvironmentClient.getStreamExecEnv();
        StreamTableEnvironment tableEnv = ExecutionEnvironmentClient.getStreamTableEnvironment(env);


        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L), new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // 将数据流转换成表
        tableEnv.createTemporaryView("EventTable", eventStream, $("user"), $("url"),$("timestamp").as("ts"));

        // 统计每个用户的点击次数
        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user");
        // 将表转换成数据流，在控制台打印输出
        tableEnv.toChangelogStream(urlCountTable).print("count");

        env.execute();




    }




}
