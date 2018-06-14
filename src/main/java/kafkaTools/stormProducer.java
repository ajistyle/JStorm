package kafkaTools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class stormProducer {
    public static void main(String[] args) {
        String topicName = "TP_ZJ32";

        Properties props = new Properties();
        props.put("bootstrap.servers", "Node1:9092");
        props.put("acks", "all");   //请求时候需要验证
        props.put("retries", 0);    // 消息发送请求失败重试次数
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);  // 消息逗留在缓冲区的时间，等待更多的消息进入缓冲区一起发送，减少请求发送次数
        props.put("buffer.memory", 33554432);// 内存缓冲区的总量
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");       //指定消息key序列化方式
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");     //指定消息本身的序列化方式

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            for(int i = 0; i < 1000000; i++) {
                Future<RecordMetadata > ftest  = producer.send(new ProducerRecord<String, String>(topicName, "key=" + Integer.toString(i), "send Value="+Integer.toString(i)));
                System.out.printf("send："+Integer.toString(i) + "\n");
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
