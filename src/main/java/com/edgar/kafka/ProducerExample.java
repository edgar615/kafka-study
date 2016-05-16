package com.edgar.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by edgar on 15-12-11.
 */
public class ProducerExample {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "10.4.7.222:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer(props);
    for(int i = 100; i < 200; i++) {
      System.out.println(i);
      Future<RecordMetadata> future = producer.send(
              new ProducerRecord<String, String>("my-topic5", Integer.toString(i),
                                                 Integer.toString(i)));
      System.out.println(future.get());

    }

    producer.close();
  }
}
