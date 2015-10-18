package com.qtagile.kafka

import spock.lang.Ignore
import spock.lang.Specification

class ConsumerSpec extends Specification {
    def static zookeeperProperties = new Properties()
    def static kafkaProperties = new Properties()
    def static consumerProperties = new Properties()

    def embeddedKafkaServer
    def producer
    def consumer

    def setupSpec(){
        zookeeperProperties.load(getClass().getClassLoader().getResourceAsStream("zookeeper.properties"))
        kafkaProperties.load(getClass().getClassLoader().getResourceAsStream("kafka.properties"))
        consumerProperties.load(getClass().getClassLoader().getResourceAsStream("consumer.properties"))
    }

    def setup(){
        embeddedKafkaServer = new EmbeddedKafkaServer(zookeeperProperties, kafkaProperties)
        embeddedKafkaServer.start()

        producer = new Producer(9090)
        consumer = new Consumer(consumerProperties, "test")
    }

    def cleanup(){
        producer.shutdown()
        consumer.shutdown()
        embeddedKafkaServer.stop()
    }

    def cleanupSpec(){
        new File('.tmp').deleteDir()
    }

    def "shouldn't get messages from an empty queue"(){
        when:
        def message = consumer.read()

        then:
        !message.isPresent()
    }

    def "should read from the queue"(){
        given:
        producer.send("test", "key", "value")

        when:
        def message = consumer.read()

        then:
        message.isPresent()
        message.get() == "value"
    }
}
