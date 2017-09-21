# spark nsq consumer
A demo to apply nsq consumer as spark consumer. Consumer receives messages from NSQd and store with spark streaming API.
Reference to dev.properties files for configuration of nsq receiver. Try playing with demo
[NSQExample](http://gitlab.qima-inc.com/bigdata/spark-nsq-consumer/blob/branch-1.0/src/test/scala/com/youzan/bigdata/streaming/example/NSQExample.scala)
Usage: NSQExample <checkpoint-directory> <parallism>.

## Maven dependency
<dependency>
  <groupId>com.youzan.bigdata</groupId>
  <artifactId>spark-streaming-nsq_2.11</artifactId>
  <version>1.0.0</version>
</dependency>


