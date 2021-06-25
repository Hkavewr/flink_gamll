package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class MyKafkaUtil {

    /**
     * 获取Kafka生产者对象
     *
     * @param topic 待写入信息的主题
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        String bootstrapServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

        return new FlinkKafkaProducer<String>(bootstrapServer,
                topic,
                new SimpleStringSchema());
    }

}
