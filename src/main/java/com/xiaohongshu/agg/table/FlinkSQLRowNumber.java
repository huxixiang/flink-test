package com.xiaohongshu.agg.table;

import com.xiaohongshu.agg.common.Event;
import com.xiaohongshu.common.flink.ExecutionEnvironmentClient;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLRowNumber {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = ExecutionEnvironmentClient.getStreamExecEnv();
        StreamTableEnvironment tableEnv = ExecutionEnvironmentClient.getStreamTableEnvironment(env);


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Event> map = source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                return new Event(value.split(",")[0], value.split(",")[1], Long.valueOf(value.split(",")[2]));
            }
        }).returns(TypeInformation.of(new TypeHint<Event>() {}));

        SingleOutputStreamOperator<Event> tableSource = map.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.eventTime)
        );

        Table sourceTable = tableEnv.fromDataStream(
                tableSource,
                $("user"),
                $("url"),
                $("eventTime").rowtime().as("ts")
        );

        tableEnv.createTemporaryView("sourceTable",sourceTable);

        Table resTable = tableEnv.sqlQuery("select user, url,ts from( select * ,row_number() over(PARTITION  by user order by ts desc) as rt from sourceTable) where rt=1");
        tableEnv.toRetractStream(resTable,Row.class).print();



        env.execute();
    }
}
