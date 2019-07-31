package com.yuanxin.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        //新建消费者组的时候需要将此属性设置为earliest，默认是latest，之所以不设置为0是因为kafka有删除消息的策略（大约7天或者大于1G的删除），
        //可能导致offset起点不是0
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //消费者自动提交offset
        props.put("enable.auto.commit", "true");
        //提交延时
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("first", "second", "third"));
        //只消费最新的数据
//        consumer.assign(Collections.singletonList(new TopicPartition("first", 0)));
        //可以指定offset来进行消费
//        consumer.seek(new TopicPartition("first", 0), 0);
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(100);
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record.topic() + "--" + record.partition() + "--" + record.value());
            }
        }
    }
}
