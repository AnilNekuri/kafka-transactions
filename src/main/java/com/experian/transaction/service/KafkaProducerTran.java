package com.experian.transaction.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.Properties;

//@Configuration
public class KafkaProducerTran {

    @Bean
    private Producer kafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092,localhost:39092,localhost:49092");
        props.put("acks", "all");
        props.put("transactional.id", "transactional-id");
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        producer.initTransactions();
        return producer;
    }

    @Bean
    private Consumer kafkaConsumer(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092,localhost:39092,localhost:49092");
        props.setProperty("group.id", "transaction");
        props.setProperty("isolation.level", "read_committed");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
//        consumer.subscribe(Arrays.asList("foo", "bar"));
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            for (ConsumerRecord<String, String> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//        }
    }
}

