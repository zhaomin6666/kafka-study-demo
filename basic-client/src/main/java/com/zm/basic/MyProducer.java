package com.zm.basic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.Arrays;
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
public class MyProducer {
    private static final String BOOTSTRAP_SERVERS = "192.168.56.100:9092,192.168.56.101:9092,192.168.56.102:9092";
    private static final String TOPIC = "test-java-client";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //PART1:设置发送者相关属性
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.zm.basic.MyInterceptor");
        // 配置key的序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // 配置value的序列化类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<>(props);
        CountDownLatch latch = new CountDownLatch(5);
        for(int i = 0; i < 5; i++) {
            //Part2:构建消息
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Integer.toString(i), "MyProducer" + i);
            //Part3:发送消息
            //单向发送：不关心服务端的应答。
//            producer.send(record);
//           System.out.println("message "+i+" sent");
            //同步发送：获取服务端应答消息前，会阻塞当前线程。
//            RecordMetadata recordMetadata = producer.send(record).get();
//            String topic = recordMetadata.topic();
//            int partition = recordMetadata.partition();
//            long offset = recordMetadata.offset();
//            String metadata = recordMetadata.toString();
//            System.out.println("metadata:["+ metadata+"] sent with topic:"+topic+"; partition:"+partition+ ";offset:"+offset);
            //异步发送：消息发送后不阻塞，服务端有应答后会触发回调函数
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(null != e){
                        System.out.println("消息发送失败,"+e.getMessage());
                        e.printStackTrace();
                    }else{
                        String topic = recordMetadata.topic();
                        long offset = recordMetadata.offset();
                        int partition = recordMetadata.partition();
                        String metadata = recordMetadata.toString();
                        System.out.println("metadata:["+ metadata+"] sent with topic:"+topic+"; partition:"+partition+ ";offset:"+offset);
                    }
                    latch.countDown();
                }
            });
        }
        //消息处理完才停止发送者。
        latch.await();
        producer.close();
    }
}
