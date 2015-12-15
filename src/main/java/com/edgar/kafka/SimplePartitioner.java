package com.edgar.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class SimplePartitioner implements Partitioner {

  private int a_numPartitions = 3;

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] Object ,
                       Cluster cluster) {
    Integer keyInt = Integer.parseInt(key.toString());
    int p = keyInt % a_numPartitions;
    System.out.println(p);
    return p;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}