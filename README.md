Kafka Offset Monitor
===========

[![Build Status](https://travis-ci.org/quantifind/KafkaOffsetMonitor.svg?branch=master)](https://travis-ci.org/quantifind/KafkaOffsetMonitor)

This is an app to monitor your kafka consumers and their position (offset) in the queue.

You can see the current consumer groups, for each group the topics that they are consuming and the position of the group in each topic queue. This is useful to understand how quick you are consuming from a queue and how fast the queue is growing. It allows for debuging kafka producers and consumers or just to have an idea of what is going on in  your system.

The app keeps an history of queue position and lag of the consumers so you can have an overview of what has happened in the last days.

Here are a few screenshots:

List of Consumer Groups
-----------------------

![Consumer Groups](http://quantifind.github.io/KafkaOffsetMonitor/img/groups.png)

List of Topics for a Group
--------------------------

![Topic List](http://quantifind.github.io/KafkaOffsetMonitor/img/topics.png)

History of Topic position
-------------------------

![Position Graph](http://quantifind.github.io/KafkaOffsetMonitor/img/graph.png)

Running It
===========

If you do not want to build it manually, just download the [current jar](https://github.com/quantifind/KafkaOffsetMonitor/releases/latest).

This is a small webapp, you can run it locally or on a server, as long as you have access to the ZooKeeper nodes controlling kafka.

```
java -cp KafkaOffsetMonitor-assembly-0.2.1.jar \
     com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --zk zk-server1,zk-server2 \
     --port 8080 \
     --refresh 10.seconds \
     --retain 2.days
     --brokerStorage true
```

The arguments are:

- **zk** the ZooKeeper hosts
- **port** on what port will the app be available
- **refresh** how often should the app refresh and store a point in the DB
- **retain** how long should points be kept in the DB
- **dbName** where to store the history (default 'offsetapp')
- **pluginsArgs** additional arguments used by extensions (see below)
- **brokerStorage** whether to use the broker storage for offsets (since Kafka 0.8.2)

Writing and using plugins
============================

Kafka Offset Monitor allows you to plug-in additional offset info reporters in case you want this information to be logged or stored somewhere. In order to write your own plugin,
all you need to do is to implement OffsetInfoReporter trait:

```
trait OffsetInfoReporter {
  def report(info: IndexedSeq[OffsetInfo])
  def cleanupOldData() = {}
}
```

It is also required, that implementation has a constructor with String as the only parameter, and this parameter will be set to pluginsArgs argument value.
Its up to you how you want to utilize this argument and configure your plugin.

When building a plugin you may find it difficult to set up dependency to Kafka Offset Monitor classes, as currently artifacts are not published to public repos.
As long as this is true you will need to use local maven repo and just publish Kafka Offset Monitor artifact with: ```sbt publishM2```

Assuming you have a custom implementation of OffsetInfoReporter in a jar file, running it is as simple as adding the jar to the classpath when running app:

```
java -cp KafkaOffsetMonitor-assembly-0.3.0.jar:kafka-offset-monitor-another-db-reporter.jar \
     com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --zk zk-server1,zk-server2 \
     --port 8080 \
     --refresh 10.seconds \
     --retain 2.days
     --brokerStorage true
     --pluginsArgs anotherDbHost=host1,anotherDbPort=555
```

For complete working example you can check [kafka-offset-monitor-graphite](https://github.com/allegro/kafka-offset-monitor-graphite), a plugin reporting offset information to Graphite.

Contributing
============

The KafkaOffsetMonitor is released under the Apache License and we **welcome any contributions** within this license. Any pull request is welcome and will be reviewed and merged as quickly as possible.

Because this open source tool is released by [Quantifind](http://www.quantifind.com) as a company, if you want to submit a pull request, you will have to sign the following simple contributors agreement:
- If you are an individual, please sign [this contributors agreement](https://docs.google.com/a/quantifind.com/document/d/1RS7qEjq3cCmJ1665UhoCMK8541Ms7KyU3kVFoO4CR_I/) and send it back to contributors@quantifind.com
- If you are contributing changes that you did as part of your work, please sign [this contributors agreement](https://docs.google.com/a/quantifind.com/document/d/1kNwLT4qG3G0Ct2mEuNdBGmKDYuApN1CpQtZF8TSVTjE/) and send it back to contributors@quantifind.com
