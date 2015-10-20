/**
 The MIT License (MIT)

 Copyright (c) 2015 QT Agile LTD http://qtagile.com/

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
 */
package com.qtagile.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

public class EmbeddedKafkaServer {
    private final KafkaLocal kafkaLocal;
    private final ZookeeperLocal zookeeperLocal;
    private final Producer producer;
    private final Properties consumerProperties;
    private final Map<String, Consumer> consumers = new HashMap<>();

    public EmbeddedKafkaServer(Properties zookeeperProperties,
                               Properties kafkaProperties,
                               Properties producerProperties,
                               Properties consumerProperties) {
        zookeeperLocal = new ZookeeperLocal(zookeeperProperties);
        kafkaLocal = new KafkaLocal(kafkaProperties);
        producer = new Producer(producerProperties);
        this.consumerProperties = consumerProperties;
    }

    public void start(){
        zookeeperLocal.start();
        kafkaLocal.start();
    }

    public void stop(){
        producer.shutdown();
        consumers.forEach((k, v) -> v.shutdown());
        kafkaLocal.stop();
        zookeeperLocal.stop();
    }

    public Future<RecordMetadata> send(String topic, String key, String value){
        return producer.send(topic, key, value);
    }

    public Optional<String> read(String topic){
        return consumers.computeIfAbsent(topic, t -> new Consumer(consumerProperties, t)).read();
    }
}
