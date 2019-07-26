package com.yuanxin.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ConsumerProducer {
    public static void main(String[] args){
        //配置文件
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "localhost:9092");
        //应答级别
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put("retries", 0);
        //批量大小，与提交延时一块决定提交消息，或者满足1毫秒或者大于16K
        props.put("batch.size", 16384);
        //提交延时
        props.put("linger.ms", 1);
        //缓存
        props.put("buffer.memory", 33554432);
        //KV的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
      producer.send(
          new ProducerRecord<String, String>("second", "key" + i, "value" + i),
          new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              if (exception == null) {
                  System.out.println("Topic：" + metadata.topic() + "--Partition：" + metadata.partition() + "--Offset：" + metadata.offset());
              }else{
                  System.out.println("发送失败");
              }
            }
          });
        }
        producer.close();
    }
}