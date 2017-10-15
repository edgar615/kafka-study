package com.edgar.kafka.limit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by edgar on 16-4-19.
 */
public class ConsumerRunnable implements Runnable {

  private String kafkaConnect;

  private String groupId;

  private String clientId;

  private String topicName;
  private long startingOffset;

  public void setStartingOffset(long startingOffset) {
    this.startingOffset = startingOffset;
  }

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
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
    //earliest: automatically reset the offset to the earliest offset
    //latest: automatically reset the offset to the latest offset
    //none: throw exception to the consumer if no previous offset is found for the consumer's group
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

    final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
    kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf("%s topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray()));
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("%s topic-partitions are assigned to this consumer\n", Arrays.toString(partitions.toArray()));
        Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
        while (topicPartitionIterator.hasNext()) {
          TopicPartition topicPartition = topicPartitionIterator.next();
          List<TopicPartition> list = new ArrayList<>();
          list.add(topicPartition);
          System.out.println("Current offset is " + kafkaConsumer.position(topicPartition) + " committed offset is ->" + kafkaConsumer.committed(topicPartition));
          if (startingOffset == -2) {
            System.out.println("Leaving it alone");
          } else if (startingOffset == 0) {
            System.out.println("Setting offset to begining");
            kafkaConsumer.seekToBeginning(list);
          } else if (startingOffset == -1) {
            System.out.println("Setting offset to end");
            kafkaConsumer.seekToEnd(list);
          } else {
            System.out.println("Resetting offset to " + startingOffset);
            kafkaConsumer.seek(topicPartition, startingOffset);
          }
        }

      }
    });
    try {
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("partition:%d, key:%s, offset:%d\n", record.partition(), record.key(), record.offset());
        }
//        if(startingOffset == -2) {
//          kafkaConsumer.commitSync();
//        }
      }
    } finally {
      kafkaConsumer.close();
    }
  }
}
