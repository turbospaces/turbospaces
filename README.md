turbospaces
===========

Features:

* Re-newal of JavaSpace technology based on disruptor/off-heap/spring API/guava/kryo libraries with user-friendly API and simple configuration
* OffHeap memory solution not bound and is not restricted by the java heap space similar to SUN's DirectBuffer
with minimal GC impact(0.2-0.5% of JVM time) capable to handle millions transactions per second on standard (pretty
cheap) environment running on any platform - pure java code without JNA or JNI.
* Guava's Cache off-heap interface implementation
* Spring's 3.1 Cache off-heap interface implementation
* In-memory object oriented transactional database with advanced access patterns(exclusive reads,locks with timeouts, etc)
* User-controlled data partitioning and native integration with databases with dynamic failover(without typical backup replications between nodes in cluster)
* Elastic external data source bindings - JPA/MongoDB/JDBC/Redis or any other storage support by spring-data.
* Distributed and persistent implementation of JavaSpace API.
* Powerful event-driven remoting via synchronous remote method invocation(don't confuse with RMI) or either
asynchronous via write/notify message delivery model.
* Support for message driven exchange pattern (integration with JMS API).
* Unlimited and linear scalability.
* Powerful web-based monitoring system and alerting engine(also can be exposed through JMX).
* Distributed Lock/Executor Service.
* Multicast and unicast discovery.
* Tight integration with spring framework (namespace, transaction management, lifecycle, events, caching layer).
* Super small: near 250 KB
* Customizable for concrete scenarios in terms of serialization, network communications, database integrations

###performance testing
turbospaces achieves 2-3M in-memory transaction on typical hardware on a single JVM node with GC impact about 0.2% of total time

##### software-hardware used

+ JDK 1.7.03
+ 2 CPU Intel(R) Xeon(R) Processor E5540 @ 2.53GHz
+ RAM 24Gb DDR2-1066
+ Oracle Enterprise Linux 5.6
+ java options: `-server -Xmx128m -XX:+HeapDumpOnOutOfMemoryError -ea -server -XX:+TieredCompilation -XX:+UseNUMA -XX:+UseCondCardMark -XX:-UseBiasedLocking -XX:+UseTLAB -XX:+DoEscapeAnalysis`
+ maven test:  `mvn test -Dtest=CombinedPerformanceTest`

##### randomize test results - typical output

* TPS = 2239266 [readsHit=60621, readsMiss=1281913, writes=448003,takesHit=20171, takesMiss=428590] 
* TPS = 2365224 [readsHit=121098, readsMiss=1298248,writes=472578, takesHit=40538, takesMiss=432775]
* TPS = 2632247 [readsHit=197404, readsMiss=1382362,writes=526969, takesHit=65452, takesMiss=460079]
