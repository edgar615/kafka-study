package com.edgar.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class SimplePartitioner implements Partitioner {

  private int a_numPartitions = 3;

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] Object ,
                       Cluster cluster) {
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    Integer keyInt = value.hashCode();
    int p = keyInt % a_numPartitions;
    System.out.println("partitionsSize:" + partitions.size() + ",p:" + p);
    return p;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}