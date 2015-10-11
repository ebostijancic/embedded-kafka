package com.qtagile.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    private final KafkaProducer<String,String> producer;

    public Producer(int serverPort){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("localhost:%d", serverPort));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public Future<RecordMetadata> send(String topic, String key, String value){
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);
        return producer.send(producerRecord);
    }

    public void shutdown(){
        producer.close();
    }
}
