package com.edgar.kafka.commit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by edgar on 16-4-19.
 */
public class Producer {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.4.7.48:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord<String, String>("test-1", "producer" + i),
              new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                  System.out.printf("partition:%d, offset:%d\n", metadata.partition(), metadata.offset());
                }
              });
    }
    producer.flush();
    producer.close();
  }
}
