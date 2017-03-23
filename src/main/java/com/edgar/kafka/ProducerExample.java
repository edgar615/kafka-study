package com.edgar.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by edgar on 15-12-11.
 */
public class ProducerExample {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "10.11.0.31:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("max.block.ms", 10000);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer(props);
    int i = 0;
//    for(; ; ) {
//      System.out.println(i);
//      Future<RecordMetadata> future = producer.send(
//              new ProducerRecord<String, String>("my-topic5", Integer.toString(i),
//                                                 Integer.toString(i)));
//      System.out.println(future.get());
//      TimeUnit.SECONDS.sleep(1);
//    }
    for (; ; ) {
      System.out.println(i);
      producer.send(
              new ProducerRecord<String, String>("my-topic5", Integer.toString(i),
                                                 Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                  System.out.println(metadata + ":" + exception);
                }
              });
      System.out.println("no");
      TimeUnit.SECONDS.sleep(1);
    }
//    producer.close();
  }
}
