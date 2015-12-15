package com.edgar.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by edgar on 15-12-11.
 */
public class DelayConsumerExample {

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.149.136:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList("delay-topic5"));
    List<PartitionInfo> partitionInfos = consumer.partitionsFor("delay-topic5");
    System.out.println(partitionInfos);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        long delay = Long.parseLong(record.value());
        long now = Instant.now().getEpochSecond();
        System.out.println(delay);
        if (delay > now) {
          System.out.println("delay");
          TimeUnit.SECONDS.sleep(delay - now);
        }
        consumer.commitSync();
      }
    }
  }
}
