package com.qtagile.kafka;

import com.google.common.collect.ImmutableMap;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Optional;
import java.util.Properties;

public class Consumer {
    private final ConsumerConnector consumerConnector;
    private final String topic;

    private KafkaStream<byte[], byte[]> stream;
    private ConsumerIterator<byte[], byte[]> iterator;

    public Consumer(String topic){
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "testgroup");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");

        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        this.topic = topic;

        stream = consumerConnector.createMessageStreams(ImmutableMap.of(topic, 1)).get(topic).get(0);
        iterator = stream.iterator();
    }

    public Optional<String> read(){
        return Optional.of(iterator)
                .filter(i -> iterator.hasNext())
                .map(i -> new String(iterator.next().message()));
    }
}
