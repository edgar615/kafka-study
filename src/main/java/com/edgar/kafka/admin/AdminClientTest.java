package com.edgar.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by Administrator on 2017/10/15.
 */
public class AdminClientTest {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "120.76.158.7:9092");
    AdminClient client = AdminClient.create(props);
      describeCluster(client);
      createTopics(client);
      listAllTopics(client);
      describeTopics(client);
      alterConfigs(client);
      describeConfig(client);
      deleteTopics(client);
  }

  /**
   * describe the cluster
   * @param client
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void describeCluster(AdminClient client) throws ExecutionException, InterruptedException {
    DescribeClusterResult ret = client.describeCluster();
    System.out.println(String.format("Cluster id: %s, controller: %s", ret.clusterId().get(), ret.controller().get()));
    System.out.println("Current cluster nodes info: ");
    for (Node node : ret.nodes().get()) {
      System.out.println(node);
    }
  }

  public static void describeConfig(AdminClient client) throws ExecutionException, InterruptedException {
    DescribeConfigsResult ret = client.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, "DeviceChangeEvent_1_3")));
    Map<ConfigResource, Config> configs = ret.all().get();
    for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
      ConfigResource key = entry.getKey();
      Config value = entry.getValue();
      System.out.println(String.format("Resource type: %s, resource name: %s", key.type(), key.name()));
      Collection<ConfigEntry> configEntries = value.entries();
      for (ConfigEntry each : configEntries) {
        System.out.println(each.name() + " = " + each.value());
      }
    }
  }

  public static void alterConfigs(AdminClient client) throws ExecutionException, InterruptedException {
    Config topicConfig = new Config(Arrays.asList(new ConfigEntry("cleanup.policy", "compact")));
    client.alterConfigs(Collections.singletonMap(
            new ConfigResource(ConfigResource.Type.TOPIC, "DeviceChangeEvent_1_3"), topicConfig)).all().get();
  }

  public static void deleteTopics(AdminClient client) throws ExecutionException, InterruptedException {
    KafkaFuture<Void> futures = client.deleteTopics(Arrays.asList("DeviceChangeEvent_1_3")).all();
    futures.get();
  }

  public static void describeTopics(AdminClient client) throws ExecutionException, InterruptedException {
    DescribeTopicsResult ret = client.describeTopics(Arrays.asList("DeviceChangeEvent_1_3", "__consumer_offsets"));
    Map<String, TopicDescription> topics = ret.all().get();
    for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
      System.out.println(entry.getKey() + " ===> " + entry.getValue());
    }
  }

  public static void createTopics(AdminClient client) throws ExecutionException, InterruptedException {
    NewTopic newTopic = new NewTopic("DeviceChangeEvent_1_3", 3, (short)3);
    CreateTopicsResult ret = client.createTopics(Arrays.asList(newTopic));
    ret.all().get();
  }

  public static void listAllTopics(AdminClient client) throws ExecutionException, InterruptedException {
    ListTopicsOptions options = new ListTopicsOptions();
    options.listInternal(true); // includes internal topics such as __consumer_offsets
    ListTopicsResult topics = client.listTopics(options);
    Set<String> topicNames = topics.names().get();
    System.out.println("Current topics in this cluster: " + topicNames);
  }
}
