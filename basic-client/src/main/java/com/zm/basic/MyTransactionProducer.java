package com.zm.basic;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * 生产者
 *
 * @author zm
 * @version 1.0
 * @date 2024-04-22
 */
public class MyTransactionProducer {
    private static final String BOOTSTRAP_SERVERS = "192.168.56.100:9092,192.168.56.101:9092,192.168.56.102:9092";
    private static final String TOPIC = "test-java-client";

    public static void main(String[] args) {
        //PART1:设置发送者相关属性
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.zm.basic.MyInterceptor");
        // 配置transaction.id
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "id_1");
        // 配置key的序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 配置value的序列化类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 开启事务
        producer.initTransactions();
        producer.beginTransaction();
        CountDownLatch latch = new CountDownLatch(5);
        try {
            for (int i = 0; i < 5; i++) {
                //Part2:构建消息
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Integer.toString(i), "MyProducer" + i);
                //Part3:发送消息
                //单向发送：不关心服务端的应答。
                producer.send(record);
                System.out.println("message " + i + " sent");
            }
            producer.commitTransaction();
        }
        catch (Exception e) {
            producer.abortTransaction();
        }
        finally {
            producer.close();
        }
    }
}
