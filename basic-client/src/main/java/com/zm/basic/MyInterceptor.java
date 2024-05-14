package com.zm.basic;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zm
 * @version 1.0
 * @date 2024-04-22
 */
public class MyInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord record) {
        System.out.println("发送时拦截：key=" + record.key() + ",partition=" + record.partition() + ",topic=" + record.topic() + ",value=" + record.value());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("发送确认时拦截：partition=" + metadata.partition() + ",topic=" + metadata.topic() + ",offset=" + metadata.offset());
    }

    @Override
    public void close() {
        System.out.println("关闭连接时拦截");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("处理配置项");
    }
}
