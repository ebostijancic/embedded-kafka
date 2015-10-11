package com.qtagile.kafka

import spock.lang.Specification

class ConsumerSpec extends Specification {
    def static producer = new Producer(9092)

    def cleanupSpec(){
        producer.shutdown()
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
