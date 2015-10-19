package com.qtagile.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    private final KafkaProducer<String,String> producer;

    public Producer(Properties properties){
        producer = new KafkaProducer<>(properties);
    }

    public Future<RecordMetadata> send(String topic, String key, String value){
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);
        return producer.send(producerRecord);
    }

    public void shutdown(){
        producer.close();
    }
}
