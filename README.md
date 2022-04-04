This is a demo service that builds windows against a stream using Goka. 

To set this up, you need to have Kafka and Zookeeper running, and you need to make three topics. 

Here are the commands I use.

```
kafka-topics                            \
        --create                        \
        --replication-factor 1          \
        --partitions 10                 \
        --config cleanup.policy=compact \
        --bootstrap-server localhost:9092 \
        --topic window-table

kafka-topics --create --topic example-stream --bootstrap-server localhost:9092
```


![alt text](https://github.com/mikedewar/aggregator/raw/master/diag.png "swimlanes diagram")



