package com.edgar.kafka.commit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by edgar on 16-4-19.
 */
public class CommitOffsetRunnable2 implements Runnable {

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
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
    kafkaConsumer.subscribe(Arrays.asList(topicName));
    System.out.println(kafkaConsumer.listTopics());
    try {
      while (true) {
//        When the group is first created, the position will be set according to the reset policy
// (which is typically either set to the earliest or latest offset for each partition). Once the
// consumer begins committing offsets, then each later rebalance will reset the position to the
// last committed offset. The parameter passed to poll controls the maximum amount of time that
// the consumer will block while it awaits records at the current position
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        for (TopicPartition partition : records.partitions()) {
          List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
          for (ConsumerRecord<String, String> record : partitionRecords) {
            System.out.println(record.offset() + ": " + record.value());
          }

          long lastoffset = partitionRecords.get(partitionRecords.size() - 1).offset();
          kafkaConsumer.commitSync(
                  Collections.singletonMap(partition, new OffsetAndMetadata(lastoffset + 1)));
        }
      }

    } finally {
      kafkaConsumer.close();
    }
  }
}
