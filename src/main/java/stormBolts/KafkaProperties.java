package stormBolts;

public interface KafkaProperties {
    String zkConnect ="Node1:2181"; //,Node2:2181,Node3:2181
    String brokers ="Node1:9092"; //,Node2:9092,Node3:9092
    String topic = "testJson";
    String topic2 = "testJson2";
    String groupId = "group2";
}
