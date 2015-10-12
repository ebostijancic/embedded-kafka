package com.qtagile.kafka

import spock.lang.Ignore
import spock.lang.Specification

class ConsumerSpec extends Specification {
    def static zookeeperProperties = new Properties()
    def static kafkaProperties = new Properties()

    def static embeddedZookeper
    def static embeddedKafka
    def static producer = new Producer(9090)

    def setupSpec(){
        zookeeperProperties.load(getClass().getClassLoader().getResourceAsStream("zookeeper.properties"))
        embeddedZookeper = new EmbeddedZookeeper(zookeeperProperties)
        embeddedZookeper.start()

        kafkaProperties.load(getClass().getClassLoader().getResourceAsStream("kafka.properties"))
        embeddedKafka = new EmbeddedKafka(kafkaProperties)
        embeddedKafka.start()
    }

    def cleanupSpec(){
        producer.shutdown()
        embeddedKafka.stop()
        embeddedZookeper.stop()
    }

    @Ignore
    def "shouldn't get messages from an empty queue"(){
        given:
        def consumer = new Consumer("test")

        when:
        def message = consumer.read()

        then:
        !message.isPresent()
    }

    def "should read from the queue"(){
        given:
        producer.send("test", "key", "value")
        def consumer = new Consumer("test")

        when:
        def message = consumer.read()

        then:
        message.isPresent()
        message.get() == "value"
    }
}
