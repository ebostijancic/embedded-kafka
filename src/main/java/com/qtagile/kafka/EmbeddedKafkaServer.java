package com.qtagile.kafka;

import java.util.Properties;

public class EmbeddedKafkaServer {
    private final EmbeddedKafka embeddedKafka;
    private final EmbeddedZookeeper embeddedZookeeper;

    public EmbeddedKafkaServer(Properties zookeeperProperties, Properties kafkaProperties) {
        embeddedZookeeper = new EmbeddedZookeeper(zookeeperProperties);
        embeddedKafka = new EmbeddedKafka(kafkaProperties);
    }

    public void start(){
        embeddedZookeeper.start();
        embeddedKafka.start();
    }

    public void stop(){
        embeddedKafka.stop();
        embeddedZookeeper.stop();
    }
}
