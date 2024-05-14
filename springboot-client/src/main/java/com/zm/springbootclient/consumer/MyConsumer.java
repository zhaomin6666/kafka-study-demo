package com.zm.springbootclient.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 消费者
 *
 * @author zm
 * @version 1.0
 * @date 2024-05-13
 */
@Component
public class MyConsumer {

    @KafkaListener(topics = {"topic1"})
    public void onMessage(ConsumerRecord<?, ?> record) {
        System.out.println("消费消息：" + record.topic() + "-" + record.partition() + "-" + record.value());
    }
}
