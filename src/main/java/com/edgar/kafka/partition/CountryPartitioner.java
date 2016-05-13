package com.edgar.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by edgar on 16-5-13.
 */
public class CountryPartitioner implements Partitioner {
  private static Map<String,Integer> countryToPartitionMap;

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    String valueStr = (String)value;
    String countryName = ((String) value).split(":")[0];
    if(countryToPartitionMap.containsKey(countryName)){
      //If the country is mapped to particular partition return it
      return countryToPartitionMap.get(countryName);
    }else {
      //If no country is mapped to particular partition distribute between remaining partitions
      int noOfPartitions = cluster.topics().size();
      return  value.hashCode()%noOfPartitions + countryToPartitionMap.size() ;
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    System.out.println("Inside CountryPartitioner.configure " + configs);
    countryToPartitionMap = new HashMap<String, Integer>();
    for(Map.Entry<String,?> entry: configs.entrySet()){
      if(entry.getKey().startsWith("partitions.")){
        String keyName = entry.getKey();
        String value = (String)entry.getValue();
        System.out.println( keyName.substring(11));
        int paritionId = Integer.parseInt(keyName.substring(11));
        countryToPartitionMap.put(value,paritionId);
      }
    }
  }
}
