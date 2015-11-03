#Embedded Kafka

It permits to run a Zookeeper and Kafka cluster for testing purposes.

##Library
Just add the following to your gradle build

```groovy
'com.qtagile:embedded-kafka:1.0.0'
```
It uses Kafka 0.8.2.2 and Zookeeper 3.4.6

##Usage

```java
private final EmbeddedKafkaServer embeddedKafkaServer = new EmbeddedKafkaServer();
```

By default is using the ports 2181 and 9090 respectively for Zookeeper and Kafka: you can override this behaviour 
passing the values on the constructor
 
```java
private final EmbeddedKafkaServer embeddedKafkaServer = new EmbeddedKafkaServer(2200, 9100);
```

After that initialize and shutdown the server at the beginning and end of the test

```java
@Before
public void setup(){
    embeddedKafkaServer.start();
}
@After
public void teardown(){
    embeddedKafkaServer.stop();
}
```

For writing a message execute

```java
embeddedKafkaServer.send(TOPIC_NAME, KEY, VALUE);
```

And for reading is simple as

```java
String messsage = embeddedKafkaServer.read(TOPIC);
```

##License
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