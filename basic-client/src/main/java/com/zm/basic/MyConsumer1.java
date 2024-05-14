package com.zm.basic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 消费者
 *
 * @author zm
 * @version 1.0
 * @date 2024-04-22
 */
public class MyConsumer1 {

    private static final String BOOTSTRAP_SERVERS = "192.168.56.100:9092,192.168.56.101:9092,192.168.56.102:9092";
    private static final String TOPIC = "test-java-client";

    public static void main(String[] args) {
        //PART1:设置消费者相关属性
        Properties props = new Properties();
        //kafka地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //每个消费者要指定一个group
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-1");
        //key序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //value序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "id1");
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // consumer.subscribe(Collections.singletonList(TOPIC));
        // 设置分配分区
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        topicPartitionList.add(new TopicPartition(TOPIC,0));
        topicPartitionList.add(new TopicPartition(TOPIC,1));
        consumer.assign(topicPartitionList);
        //自行调整Offset
        consumer.seekToBeginning(topicPartitionList);
        //consumer.seek(new TopicPartition(TOPIC,0), 0);
        while (true) {
            //PART2:拉取消息
            // 100毫秒超时时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofNanos(100));
//            records.partitions().forEach(topicPartition -> {
//                String key = topicPartition.topic()+topicPartition.partition();
//                List<ConsumerRecord<String, String>> partionRecords = records.records(topicPartition);
//                long value = partionRecords.get(partionRecords.size()-1).offset();
//
//            });
            //PART3:处理消息
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("partition = " + record.partition() + "; offset = " + record.offset() + "; key = " + record.key() + "; value= " + record.value());
            }


            //提交offset，消息就不会重复推送。
            consumer.commitSync(); //同步提交，表示必须等到offset提交完毕，再去消费下一批数据。
//            consumer.commitAsync(); //异步提交，表示发送完提交offset请求后，就开始消费下一批数据了。不用等到Broker的确认。
        }
    }

}
