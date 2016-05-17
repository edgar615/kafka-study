package com.edgar.kafka.assign;


/**
 * Created by edgar on 16-4-19.
 */
public class Consumer {
  public static void main(String[] args) {
    CommitAssignRunnable runnable = new CommitAssignRunnable();
    runnable.setClientId("simple");
    runnable.setGroupId("test");
    runnable.setKafkaConnect("10.4.7.48:9092");
    runnable.setTopicName("topic-6");
    new Thread(runnable).start();
  }
}
