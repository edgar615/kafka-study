package com.edgar.kafka.pause;


/**
 * Created by edgar on 16-4-19.
 */
public class Consumer {
  public static void main(String[] args) {
    ConsumerRunnable runnable = new ConsumerRunnable();
    runnable.setClientId("simple");
    runnable.setGroupId("test2");
    runnable.setKafkaConnect("test.ihorn.com.cn:9092");
    runnable.setTopicName("niot-ihorn-hanf");
    runnable.setStartingOffset(44096);
    new Thread(runnable).start();
  }
}
