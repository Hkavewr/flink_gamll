package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static final String BOOT_STRAPSERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static final String DWD_DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    /**
     * 获取Kafka生产者对象
     *
     * @param topic 待写入信息的主题
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        return new FlinkKafkaProducer<String>(BOOT_STRAPSERVER,
                topic,
                new SimpleStringSchema());
    }


    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAPSERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);

    }


    /**
     * 获取Kafka生产者对象
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerWithSchema(KafkaSerializationSchema<T> serializationSchema) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAPSERVER);

        return new FlinkKafkaProducer<T>(DWD_DEFAULT_TOPIC,
                serializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

}
