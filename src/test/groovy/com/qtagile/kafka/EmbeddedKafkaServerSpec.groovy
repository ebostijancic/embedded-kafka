package com.qtagile.kafka

import spock.lang.Specification

class EmbeddedKafkaServerSpec extends Specification {
    def static zookeeperProperties = new Properties()
    def static kafkaProperties = new Properties()
    def static producerProperties = new Properties()
    def static consumerProperties = new Properties()

    def embeddedKafkaServer

    def setupSpec(){
        zookeeperProperties.load(getClass().getClassLoader().getResourceAsStream("zookeeper.properties"))
        kafkaProperties.load(getClass().getClassLoader().getResourceAsStream("kafka.properties"))
        producerProperties.load(getClass().getClassLoader().getResourceAsStream("producer.properties"))
        consumerProperties.load(getClass().getClassLoader().getResourceAsStream("consumer.properties"))
    }

    def setup(){
        embeddedKafkaServer = new EmbeddedKafkaServer(zookeeperProperties, kafkaProperties,
                                                      producerProperties, consumerProperties)
        embeddedKafkaServer.start()
    }

    def cleanup(){
        embeddedKafkaServer.stop()
    }

    def cleanupSpec(){
        new File('.tmp').deleteDir()
    }

    def "shouldn't get messages from an empty queue"(){
        when:
        def message = embeddedKafkaServer.read('test')

        then:
        !message.isPresent()
    }

    def "should read a message from the queue"(){
        given:
        embeddedKafkaServer.send("test", "key", "value")

        when:
        def message = embeddedKafkaServer.read('test')

        then:
        message.isPresent()
        message.get() == "value"
    }
}
