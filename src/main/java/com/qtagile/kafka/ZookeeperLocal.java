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

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZookeeperLocal {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperLocal.class);

    private final ZooKeeperServerMain zooKeeperServer;
    private final ServerConfig configuration;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public ZookeeperLocal(Properties properties) {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();

        try {
            quorumConfiguration.parseProperties(properties);
        } catch (Exception e) {
            logger.error("zookeeper local configuration error", e);
            throw new RuntimeException(e);
        }

        configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);
        zooKeeperServer = new ZooKeeperServerMain();
    }

    public void start(){
        executorService.submit(() -> {
            try {
                zooKeeperServer.runFromConfig(configuration);
            } catch (IOException e) {
                logger.error("zookeeper local configuration error", e);
            }
        });
    }

    public void stop(){
        executorService.shutdown();
    }
}
