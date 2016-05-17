package com.edgar.kafka.commit;


/**
 * Created by edgar on 16-4-19.
 */
public class Consumer {
  public static void main(String[] args) {
    CommitOffsetRunnable runnable = new CommitOffsetRunnable();
    runnable.setClientId("simple");
    runnable.setGroupId("test");
    runnable.setKafkaConnect("10.4.7.48:9092");
    runnable.setTopicName("test-1");
    new Thread(runnable).start();
  }
}
