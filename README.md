# spark nsq consumer
A demo to apply nsq consumer as spark consumer. Consumer receives messages from NSQd and store with spark streaming API.
Reference to dev.properties files for configuration of nsq receiver. Try playing with demo
[NSQExample](http://gitlab.qima-inc.com/bigdata/spark-nsq-consumer/blob/branch-1.0/src/test/scala/com/youzan/bigdata/streaming/example/NSQExample.scala)
<br>Usage: <br>
```NSQExample <checkpoint-directory> <parallism>```

## Maven dependency
```
<dependency>
  <groupId>com.youzan.bigdata</groupId>
  <artifactId>spark-streaming-nsq_2.11</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Instrument
This connector consists of two version
1. Unreliable
2. Reliable

The difference of the two is whether "spark.streaming.receiver.writeAheadLog.enable" is set to true.
unreliable version is set to false and let the client ack messages automatically, and reliable one write messages
via WALog, and ack messages once messages are stored to disk. <br>

One parameter needs mention is <b>nsq.rdy</b>, it works as throttle strategy.
