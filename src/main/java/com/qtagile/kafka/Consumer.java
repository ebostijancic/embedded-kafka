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

import com.google.common.collect.ImmutableMap;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final ConsumerConnector consumerConnector;
    private ConsumerIterator<byte[], byte[]> iterator;

    public Consumer(Properties consumerProperties, String topic){
        consumerConnector = kafka.consumer.Consumer
                .createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
        KafkaStream<byte[], byte[]> stream = consumerConnector
                .createMessageStreams(ImmutableMap.of(topic, 1)).get(topic).get(0);
        iterator = stream.iterator();
    }

    public void shutdown(){
        logger.debug("shutdown consumer");
        consumerConnector.shutdown();
    }

    public Optional<String> read(){
        return Optional.of(iterator)
                .filter(this::hasMessage)
                .map(i -> new String(iterator.next().message()));
    }

    public boolean hasMessage(ConsumerIterator<byte[], byte[]> iterator){
        try {
            return iterator.hasNext();
        } catch(ConsumerTimeoutException cte){
            logger.debug("no message found in the queue", cte);
            return false;
        }
    }
}
