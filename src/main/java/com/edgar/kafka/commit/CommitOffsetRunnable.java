package com.edgar.kafka.commit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by edgar on 16-4-19.
 */
public class CommitOffsetRunnable implements Runnable {

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
        for (ConsumerRecord<String, String> record : records) {
          TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
          System.out.println("Current offset is " + kafkaConsumer.position(topicPartition)
                             + " committed offset is ->" + kafkaConsumer.committed(topicPartition));
          System.out.printf("partition:%d, key:%s, offset:%d\n", record.partition(), record.key(),
                            record.offset());

//          n this example, we've passed the explicit offset we want to commit in the call to
// commitSync. The committed offset should always be the offset of the next message that your
// application will read. When commitSync is called with no arguments, the consumer commits the
// last offsets (plus one) that were returned to the application, but we can't use that here
// since that since it would allow the committed position to get ahead of our actual progress.
          kafkaConsumer.commitAsync(
                  Collections
                          .singletonMap(topicPartition, new
                                  OffsetAndMetadata(record.offset() + 1)),
                  new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           Exception exception) {

                    }
                  });
        }
      }
    } finally {
      kafkaConsumer.close();
    }
  }
}
