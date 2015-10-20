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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaLocal {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLocal.class);

    private final KafkaServerStartable kafkaServer;

    public KafkaLocal(Properties properties){
        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        kafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    public void start(){
        logger.debug("start kafka local");
        kafkaServer.startup();
        logger.debug("kafka local started");
    }

    public void stop(){
        logger.debug("stop kafka local");
        kafkaServer.shutdown();
        logger.debug("kafka local stopped");
    }
}
