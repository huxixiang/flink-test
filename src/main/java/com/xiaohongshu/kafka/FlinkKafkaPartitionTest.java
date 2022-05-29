package com.xiaohongshu.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaPartitionTest {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<byte[]> map = source.map(new MapFunction<String, byte[]>() {
            @Override
            public byte[] map(String s) throws Exception {
                return s.getBytes();
            }
        });

        map.addSink(KafkaPartitionProducer.getKafkaProducer());
        env.execute();
    }
}
