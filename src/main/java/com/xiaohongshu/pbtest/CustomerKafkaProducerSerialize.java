package com.xiaohongshu.pbtest;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class CustomerKafkaProducerSerialize implements SerializationSchema<byte[]> {
    @Override
    public byte[] serialize(byte[] bytes) {
        return bytes;
    }
}
