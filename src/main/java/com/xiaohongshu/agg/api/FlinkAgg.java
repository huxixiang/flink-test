package com.xiaohongshu.agg.api;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Iterator;

/**
 * 除了这个接口AggregateFunction，
 * flink中还有一个抽象类AggregateFunction：org.apache.flink.table.functions.AggregateFunction，
 * 大家不要把这个弄混淆了，接口AggregateFunction我们可以理解为flink中的一个算子，和MapFunction、FlatMapFunction等是同级别的，
 * 而抽象类AggregateFunction是用于用户自定义聚合函数的，和max、min之类的函数是同级的。
 */
public class FlinkAgg {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new MySourceFunction());
//        source.print();
//
        source.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new CountAggregate(), new WindowResult()).print();

        env.execute();

    }



    //自定义source函数
    public static class MySourceFunction implements SourceFunction<Tuple2<String,Long>>{
        String userids[] = {
                "4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
                "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
                "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
                "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
                "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
        };
        private  static boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
            while (isRunning){
                Thread.sleep(100);
                String userid = userids[(int) (Math.random() * (userids.length - 1))];
                sourceContext.collect(Tuple2.of(userid, System.currentTimeMillis()));
            }
        }
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    //自定义聚合函数
    public static class CountAggregate implements AggregateFunction<Tuple2<String,Long>,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Long> input, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer output) {
            return output;
        }

        @Override
        public Integer merge(Integer record, Integer acc) {
            return acc+record;
        }
    }

    //自定义结果输出函数

    public static class WindowResult implements WindowFunction<Integer, Tuple3<String, Date,Integer>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Integer> input, Collector<Tuple3<String, Date, Integer>> out) throws Exception {
            String k = ((Tuple1<String>)key).f0;
            long windowStart = window.getStart();
            int result = 0;
            int tmp = 0;
            Iterator iterator = input.iterator();
            while (iterator.hasNext()){
                tmp = (int)iterator.next();
                if(result==0){
                    result = tmp;
                }

                System.out.println("tmp======:"+tmp);
            }
            out.collect(Tuple3.of(k, new Date(windowStart), result));
        }
    }


}
