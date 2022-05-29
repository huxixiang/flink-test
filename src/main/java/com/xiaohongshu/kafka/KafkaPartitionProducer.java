package com.xiaohongshu.kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Properties;

public class KafkaPartitionProducer {

    public static FlinkKafkaProducer getKafkaProducer(){
        return new FlinkKafkaProducer(
                "FLINK_TOPIC_TEST",
                new MySchema(),
                getProperties(),
                java.util.Optional.of(new KafkaPartitioner())
        );
    }


    public static Properties getProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;

    }

    public static class MySchema implements KeyedSerializationSchema<byte[]>{
        //element: 具体数据
        /**
         * 要发送的key
         *
         * @param element 原数据
         * @return key.getBytes
         */
        @Override
        public byte[] serializeKey(byte[] element) {
            //这里可以随便取你想要的key，然后下面分区器就根据这个key去决定发送到kafka哪个分区中，
            //element就是flink流中的真实数据，取出key后要转成字节数组
            return element.toString().split("@")[0].getBytes();
        }

        /**
         * 要发送的value
         *
         * @param element 原数据
         * @return value.getBytes
         */
        @Override
        public byte[] serializeValue(byte[] element) {
            //要序列化的value，这里一般就原封不动的转成字节数组就行了
            return element.toString().getBytes();
        }

        //这里返回要发送的topic名字，没什么用，可以不做处理
        @Override
        public String getTargetTopic(byte[] element) {
            return null;
        }
    }



    public static class KafkaPartitioner extends FlinkKafkaPartitioner{
        /**
         * @param record      正常的记录
         * @param key         KeyedSerializationSchema中配置的key
         * @param value       KeyedSerializationSchema中配置的value
         * @param topic         topic
         * @param partitions  partition列表[0, 1, 2, 3, 4]
         * @return partition
         */
        @Override
        public int partition(Object record, byte[] key, byte[] value, String topic, int[] partitions) {
            //这里接收到的key是上面MySchema()中序列化后的key，需要转成string，然后取key的hash值`%`上kafka分区数量
            return Math.abs(new String(key).hashCode() % partitions.length);
        }
    }



}
