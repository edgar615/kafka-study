package com.edgar.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;

/**
 * Created by edgar on 15-12-11.
 */
public class DelayProducerExample {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.149.136:9092");

    //The acks config controls the criteria under which requests are considered complete.
    //The "all" setting we have specified will result in blocking on the full commit of the
    // record, the slowest but most durable setting.
    props.put("acks", "all");
    //If the request fails, the producer can automatically retry, though since we have specified
    // retries as 0 it won't.
    // Enabling retries also opens up the possibility of duplicates (see the documentation on
    // message delivery semantics for details).
    props.put("retries", 0);
    //The producer maintains buffers of unsent records for each partition. These buffers are of a
    // size specified by the batch.size config.
    // Making this larger can result in more batching, but requires more memory (since we will
    // generally have one of these buffers for each active partition).
    props.put("batch.size", 16384);
    //By default a buffer is available to send immediately even if there is additional unused
    // space in the buffer.
    // However if you want to reduce the number of requests you can set linger.ms to something
    // greater than 0.
    // This will instruct the producer to wait up to that number of milliseconds before sending a
    // request in hope that more records will arrive to fill up the same batch.
    // This is analogous to Nagle's algorithm in TCP.
    // For example, in the code snippet above, likely all 100 records would be sent in a single
    // request since we set our linger time to 1 millisecond.
    // However this setting would add 1 millisecond of latency to our request waiting for more
    // records to arrive if we didn't fill up the buffer. Note that records that arrive close
    // together in time will generally batch together even with linger.ms=0 so under heavy load
    // batching will occur regardless of the linger configuration;
    // however setting this to something larger than 0 can lead to fewer, more efficient requests
    // when not under maximal load at the cost of a small amount of latency.
    props.put("linger.ms", 1);
//    The buffer.memory controls the total amount of memory available to the producer for
// buffering. If records are sent faster than they can be transmitted to the server then this
// buffer space will be exhausted. When the buffer space is exhausted additional send calls will
// block. For uses where you want to avoid any blocking you can set block.on.buffer.full=false
// which will cause the send call to result in an exception.
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    //分区
//    props.put("partitioner.class", "com.edgar.kafka.SimplePartitioner");

    Producer<String, String> producer = new KafkaProducer(props);
    for (int i = 100; i < 200; i++) {
      System.out.println(i);
      long delay = Instant.now().getEpochSecond() + 5;
      producer.send(new ProducerRecord<String, String>("delay-topic5", Integer.toString(i),
                                                       String.valueOf(delay)));

    }

    producer.close();
  }
}
