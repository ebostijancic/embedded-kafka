package com.qtagile.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class EmbeddedKafka {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafka.class);

    private final KafkaServerStartable kafkaServer;

    public EmbeddedKafka(Properties properties){
        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        kafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    public void start(){
        logger.debug("start kafka server");
        kafkaServer.startup();
        logger.debug("kafka server started");
    }

    public void stop(){
        logger.debug("stop kafka server");
        kafkaServer.shutdown();
        logger.debug("kafka server stopped");
    }
}
