package com.qtagile.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

public class EmbeddedKafkaServer {
    private final EmbeddedKafka embeddedKafka;
    private final EmbeddedZookeeper embeddedZookeeper;
    private final Producer producer;
    private final Properties consumerProperties;
    private Map<String, Consumer> consumers = new HashMap<>();

    public EmbeddedKafkaServer(Properties zookeeperProperties,
                               Properties kafkaProperties,
                               Properties producerProperties,
                               Properties consumerProperties) {
        embeddedZookeeper = new EmbeddedZookeeper(zookeeperProperties);
        embeddedKafka = new EmbeddedKafka(kafkaProperties);
        producer = new Producer(producerProperties);
        this.consumerProperties = consumerProperties;
    }

    public void start(){
        embeddedZookeeper.start();
        embeddedKafka.start();
    }

    public void stop(){
        producer.shutdown();
        consumers.forEach((k, v) -> v.shutdown());
        embeddedKafka.stop();
        embeddedZookeeper.stop();
    }

    public Future<RecordMetadata> send(String topic, String key, String value){
        return producer.send(topic, key, value);
    }

    public Optional<String> read(String topic){
        return consumers.computeIfAbsent(topic, t -> new Consumer(consumerProperties, t)).read();
    }
}
