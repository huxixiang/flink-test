package com.xiaohongshu.kafka.consumer;

import com.xiaohongshu.common.flink.ExecutionEnvironmentClient;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * flink kafka consumer 几种消费模式
 */
public class FlinkKafkaConsumerOffset {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = ExecutionEnvironmentClient.getStreamExecEnv();
        env.enableCheckpointing(5000);
        env.disableOperatorChaining();


        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka 消息的value序列化器
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组
        properties.put("group.id", "consumer_test");
        // 自动偏移量提交
//        properties.put("enable.auto.commit", true);
        // 偏移量提交的时间间隔，毫秒
//        properties.put("auto.commit.interval.ms", 5000);
        // 指定kafka的消费者从哪里开始消费数据
        // 共有三种方式，
        // #earliest
        // 当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，从头开始消费
        // #latest
        // 当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，消费新产生的该分区下的数据
        // #none
        // topic各分区都存在已提交的offset时，
        // 从offset后开始消费；
        // 只要有一个分区不存在已提交的offset，则抛出异常
//        properties.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "consumer_test",
                new SimpleStringSchema(),
                properties);
        //设置checkpoint后在提交offset，即oncheckpoint模式
        // 该值默认为true，
        consumer.setCommitOffsetsOnCheckpoints(true);

        // 最早的数据开始消费
        // 该模式下，Kafka 中的 committed offset 将被忽略，不会用作起始位置。
        consumer.setStartFromEarliest();

        // 消费者组最近一次提交的偏移量，默认。
        // 如果找不到分区的偏移量，那么将会使用配置中的 auto.offset.reset 设置
        //consumer.setStartFromGroupOffsets();

        // 最新的数据开始消费
        // 该模式下，Kafka 中的 committed offset 将被忽略，不会用作起始位置。
//        consumer.setStartFromLatest();

        // 指定具体的偏移量时间戳,毫秒
        // 对于每个分区，其时间戳大于或等于指定时间戳的记录将用作起始位置。
        // 如果一个分区的最新记录早于指定的时间戳，则只从最新记录读取该分区数据。
        // 在这种模式下，Kafka 中的已提交 offset 将被忽略，不会用作起始位置。
        //consumer.setStartFromTimestamp(1585047859000L);

        // 为每个分区指定偏移量
        /*Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("topic_name", 0), 23L);
        specificStartOffsets.put(new KafkaTopicPartition("topic_name", 1), 31L);
        specificStartOffsets.put(new KafkaTopicPartition("topic_name", 2), 43L);
        consumer1.setStartFromSpecificOffsets(specificStartOffsets);*/
        /**
         *
         * 请注意：当 Job 从故障中自动恢复或使用 savepoint 手动恢复时，
         * 这些起始位置配置方法不会影响消费的起始位置。
         * 在恢复时，每个 Kafka 分区的起始位置由存储在 savepoint 或 checkpoint 中的 offset 确定
         *
         */

        DataStreamSource<String> source = env.addSource(consumer);
        // TODO
        source.print();
        env.execute("test kafka connector");


    }
}
