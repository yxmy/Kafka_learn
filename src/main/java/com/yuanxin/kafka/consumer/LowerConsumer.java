package com.yuanxin.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据topic， partition， offset获取数据
 */
public class LowerConsumer {

    public static void main(String[] args) {

        //设置地址
        String broker = "localhost";

        //设置端口号
        List<Integer> ports = new ArrayList<Integer>();
        ports.add(9092);
        ports.add(9093);
        ports.add(9094);

        //设置topic
        String topic = "first";

        //设置partition
        int partition = 1;

        //设置offset
        int offset = 0;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(broker, ports, topic, partition, offset);
    }

    /**
     * 获取leader
     * @param broker
     * @param ports
     * @param topic
     * @param partition
     * @return
     */
    private BrokerEndPoint findLeader(String broker, List<Integer> ports, String topic, int partition) {
        for (Integer port : ports) {
            SimpleConsumer leader = new SimpleConsumer(broker, port, 100, 1024 * 4, "findLeader");
            //获取元数据请求
            TopicMetadataRequest metadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            TopicMetadataResponse metadataResponse = leader.send(metadataRequest);
            //解析topic元数据相应，获取topic元数据
            List<TopicMetadata> topicMetadata = metadataResponse.topicsMetadata();
            for (TopicMetadata metadata : topicMetadata) {
                //获取多个分区的元数据信息
                List<PartitionMetadata> partitionMetadataList = metadata.partitionsMetadata();
                //遍历分区元数据
                for (PartitionMetadata partitionMetadata : partitionMetadataList) {
                    if (partitionMetadata.partitionId() == partition) {
                        return partitionMetadata.leader();
                    }
                }
            }
        }
        return null;
    }

    /**
     * 获取数据
     * @param broker
     * @param ports
     * @param topic
     * @param partition
     * @param offset
     */
    private void getData(String broker, List<Integer> ports, String topic, int partition, int offset) {
        //获取到topic 和partition 的leader
        BrokerEndPoint brokerEndPoint = findLeader(broker, ports, topic, partition);
        String host = brokerEndPoint.host();
        int port = brokerEndPoint.port();
        System.out.println("leader---" + host + ":" + port);
        SimpleConsumer simpleConsumer = new SimpleConsumer(host, port, 1000, 1024 * 4, "getData");

        //构造获取数据所需的fetchRequest
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000000).build();
        FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        System.out.println("-------------------message-----------------");
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte [] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1 + "--" + new String(bytes));
        }

    }
}
