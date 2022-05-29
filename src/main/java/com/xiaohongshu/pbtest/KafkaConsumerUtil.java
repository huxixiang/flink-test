package com.xiaohongshu.pbtest;

import com.xiaohongshu.ads.service.log.schema.AdsServiceLog;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerUtil {




    public static FlinkKafkaConsumer<ConsumerRecord<byte[],byte[]>>getFlinkKafkaConsumer() {
        return new FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>>(KafkaProducerUtil.SINK_TOPIC, new KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>>() {
            @Override
            public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> record) {
                return false;
            }

            @Override
            public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                return consumerRecord;
            }

            @Override
            public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
                return TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() {
                });
            }
        },getKafkaConfig());

    }


    public static Properties getKafkaConfig(){
        Properties  props=new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","pb_test_01");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset","earliest");
        return props;
    }

}
