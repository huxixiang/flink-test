package com.xiaohongshu.agg.table;

import com.xiaohongshu.agg.common.Event;
import com.xiaohongshu.common.flink.ExecutionEnvironmentClient;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableTumbleApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = ExecutionEnvironmentClient.getStreamExecEnv();
        StreamTableEnvironment tableEnv = ExecutionEnvironmentClient.getStreamTableEnvironment(env);
        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1",  2 * 1000L),
                        new Event("Alice", "./prod?id=4", 2 * 1000L),
                        new Event("Bob", "./prod?id=5", 4 * 1000L),
                        new Event("Cary", "./home", 5 * 1000L),
                        new Event("Cary", "./prod?id=7", 7 * 1000L)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                );

        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // 为方便在SQL中引用，在环境中注册表EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 设置累积窗口，执行SQL统计查询
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "user, " +
                        "COUNT(url) AS cnt ," +    // 统计 url 访问次数
                        "window_start as startT,"+
                        "window_end as endT "+
                        "FROM TABLE( " +
                        "TUMBLE( TABLE EventTable, " +    // 1 小时滚动窗口
                        "DESCRIPTOR(ts), " + "INTERVAL '3' SECOND)) " +
                        "GROUP BY user, window_start, window_end "
        );

        tableEnv.toDataStream(result).print();

        env.execute();
    }
}
