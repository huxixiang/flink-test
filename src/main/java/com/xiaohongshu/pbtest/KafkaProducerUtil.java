package com.xiaohongshu.pbtest;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class KafkaProducerUtil {

    public static String SINK_TOPIC = "pb_producer_topic";


    public static FlinkKafkaProducer<byte[]>getKafkaProducer(){
        return new FlinkKafkaProducer<byte[]>(
                "localhost:9092",
                SINK_TOPIC,
                new CustomerKafkaProducerSerialize()
        );
    }



}
