package com.edgar.kafka.partition;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by edgar on 16-4-19.
 */
public class Producer {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.4.7.48:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    System.out.println(CountryPartitioner.class.getCanonicalName());
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getCanonicalName());

    props.put("partitions.0","USA");
    props.put("partitions.1","India");

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    String topicName = "topic-6";
    Scanner in = new Scanner(System.in);
    System.out.println("Enter message(type exit to quit)");
    String line = in.nextLine();
    while (!"quit".equalsIgnoreCase(line)) {
      System.out.println(line);
      producer.send(new ProducerRecord<String, String>(topicName, line),
              new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                  System.out.printf("partition:%d, offset:%d\n", metadata.partition(), metadata.offset());
                }
              });
//      producer.flush();
      line = in.nextLine();
    }

    producer.flush();
    producer.close();
    in.close();
  }
}
