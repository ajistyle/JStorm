package kafkaTools;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/3/26
 * @since 1.0.0
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ConsumerThread implements Runnable {
    private static KafkaConsumer<String,String> kafkaConsumer;
    private final String topic;
    private static Queue<String> queue = new ConcurrentLinkedDeque<String>();

    public ConsumerThread(String brokers, String groupId, String topic){
        Properties properties = buildKafkaProperty(brokers,groupId);
        this.topic = topic;
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties buildKafkaProperty(String brokers,String groupId){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String,String> item : consumerRecords){
                queue.add(item.value());
//                System.err.println("Consumer Message:"+item.value()+",Partition:"+item.partition()+" Offset:"+item.offset() + " queue_size:"+queue.size());
            }
        }
    }

    public static Queue<String> getQueue()
    {
        return queue;
    }
}