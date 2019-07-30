package com.yuanxin.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        //消费者自动提交offset
        props.put("enable.auto.commit", "true");
        //提交延时
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("first", "second", "third"));
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(100);
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record.topic() + "--" + record.partition() + "--" + record.value());
            }
        }
    }
}
