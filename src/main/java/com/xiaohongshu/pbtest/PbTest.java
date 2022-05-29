package com.xiaohongshu.pbtest;

import com.xiaohongshu.ads.service.log.schema.AdsServiceLog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.UUID;

public class PbTest {
    public static void main(String[] args)throws Exception {
        Configuration configuration = new Configuration();
        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<byte[]> sinkStream = source.map(new MapFunction<String, byte[]>() {
            @Override
            public byte[] map(String s) throws Exception {
                System.out.println("=============="+s+"===============");
                String uuid = UUID.randomUUID().toString().replaceAll("-", "").toLowerCase();
                AdsServiceLog.AdsServiceLogInfo.Builder adsServiceLogInfoBuilder = AdsServiceLog.AdsServiceLogInfo.newBuilder();
                AdsServiceLog.AdsInfo.Builder adsInfoBuilder = AdsServiceLog.AdsInfo.newBuilder();
                adsInfoBuilder.setAdsId(1);
                adsInfoBuilder.setAdsUuid(uuid);
                adsInfoBuilder.setAdsType(AdsServiceLog.AdsType.GD_ADS);
                adsServiceLogInfoBuilder.addAdsInfo(adsInfoBuilder.build());
                byte[] bytes = adsServiceLogInfoBuilder.build().toByteArray();
                System.out.println("bytes:"+bytes);
                return bytes;
            }
        });
        // 序列化后的数据写入到kafka中
        FlinkKafkaProducer<byte[]> kafkaProducer = KafkaProducerUtil.getKafkaProducer();
        sinkStream.addSink(kafkaProducer);

//
//        //从kafka中读取并反序列化
        FlinkKafkaConsumer<ConsumerRecord<byte[],byte[]>> kafkaConsumer = KafkaConsumerUtil.getFlinkKafkaConsumer();


        DataStreamSource<ConsumerRecord<byte[],byte[]>> sourcePb = env.addSource(kafkaConsumer);

        sourcePb.flatMap(new FlatMapFunction<ConsumerRecord<byte[],byte[]>, String>() {
            @Override
            public void flatMap(ConsumerRecord<byte[], byte[]> record, Collector<String> collector) throws Exception {
                System.out.println(AdsServiceLog.AdsServiceLogInfo.parseFrom(record.value()).getAdsInfo(0));
                collector.collect(""+AdsServiceLog.AdsServiceLogInfo.parseFrom(record.value()).getAdsInfo(0));



            }
        }).print();


        env.execute();
    }
}
