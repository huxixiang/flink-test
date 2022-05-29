package com.xiaohongshu.pbtest;

import com.xiaohongshu.ads.service.log.schema.AdsServiceLog;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class CustomerKafkaConsumerDeserialization implements KafkaDeserializationSchema<AdsServiceLog.AdsServiceLogInfo>{
    @Override
    public boolean isEndOfStream(AdsServiceLog.AdsServiceLogInfo adsServiceLogInfo) {
        return false;
    }

    @Override
    public AdsServiceLog.AdsServiceLogInfo deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return AdsServiceLog.AdsServiceLogInfo.parseFrom(consumerRecord.value());
    }

    @Override
    public TypeInformation<AdsServiceLog.AdsServiceLogInfo> getProducedType() {
        return TypeInformation.of(new TypeHint<AdsServiceLog.AdsServiceLogInfo>() {
        });
    }
}