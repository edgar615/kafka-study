package com.edgar.kafka.partition;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * Created by edgar on 16-4-19.
 */
public class ConsumerRunnable implements Runnable {

  private String kafkaConnect;

  private String groupId;

  private String clientId;

  private String topicName;

  public void setKafkaConnect(String kafkaConnect) {
    this.kafkaConnect = kafkaConnect;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  @Override
  public void run() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//    props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
//    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
    kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf("%s topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray()));
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("%s topic-partitions are assigned to this consumer\n", Arrays.toString(partitions.toArray()));
      }
    });
    try {
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        for (ConsumerRecord<String, String> record : records)
          System.out.println(record);
      }
    } catch (WakeupException ex) {
      System.out.println("Exception caught " + ex.getMessage());
    } finally {
      kafkaConsumer.close();
      System.out.println("After closing KafkaConsumer");
    }
  }
}
