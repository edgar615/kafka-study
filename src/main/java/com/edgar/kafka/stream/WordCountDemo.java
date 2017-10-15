package com.edgar.kafka.stream;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/10/15.
 */
public class WordCountDemo {
  public static void main(String[] args) {
//    KStreamBuilder builder = new KStreamBuilder();
//    KStream<String, String> source = builder.stream("streams-plaintext-input");
    Pattern pattern = Pattern.compile("^DeviceControlEvent\\d+$");
    Matcher matcher = pattern.matcher("DeviceControlEvent_1_3");
    System.out.println(matcher.matches());
  }
}
