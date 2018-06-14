package kafkaTools;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import stormBolts.KafkaProperties;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/6/11
 * @since 1.0.0
 */
public class stormConsumer extends Thread {
    private ConsumerConnector consumer;
    private String topic;

    private Queue<String> queue = new ConcurrentLinkedDeque<String>();

    public stormConsumer(String topic){
//        consumer= kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic=topic;
    }

    private static ConsumerConfig createConsumerConfig(){
        Properties props= new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id",KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms",400);
        props.put("zookeeper.sync.timeout.ms",200);
        props.put("auto.commit.interval.ms",6000);
        return new ConsumerConfig(props);
    }

    public void init(){

        Properties props = new Properties();
        /**配置*/
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
//        必须的配置， 代表该消费者所属的 consumer group
        props.put("group.id", KafkaProperties.groupId);
//        多长时间没有发送心跳信息到zookeeper就会认为其挂掉了，默认是6000
        props.put("zookeeper.session.timeout.ms", "6000");
//        可以允许zookeeper follower 比 leader慢的时长
        props.put("zookeeper.sync.time.ms", "200");
//        控制consumer offsets提交到zookeeper的频率， 默认是60 * 1000
        props.put("auto.commit.interval.ms", "1000");

        props.put("bootstrap.servers", "Node1:9092");
        props.put("acks", "all");   //请求时候需要验证
        props.put("retries", 0);    // 消息发送请求失败重试次数
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);  // 消息逗留在缓冲区的时间，等待更多的消息进入缓冲区一起发送，减少请求发送次数
        props.put("buffer.memory", 33554432);// 内存缓冲区的总量
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");       //指定消息key序列化方式
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");     //指定消息本身的序列化方式


        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        /**
         * createMessageStreams 为每个topic创建 message stream
         */
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            try {
                String message = new String(iterator.next().message());
                queue.add(new String(message));

                System.err.println("收到消息" + message);
                Thread.sleep(5000);
            } catch (Throwable e) {
                System.out.println(e.getCause());
            }
        }

    }

    public void zzzz() {
        System.out.println("开始消费消息...");
        Executors.newSingleThreadExecutor().execute(new Runnable() {

            public void run() {
                init();
                while (true) {
                    try {
                        run();
                    } catch (Throwable e) {
                        if (consumer != null) {
                            try {
                                consumer.shutdown();
                            } catch (Throwable e1) {
                                System.out.println("Turn off Kafka consumer error! " + e);
                            }
                        }
                    }
                }
            }
        });
    }

    public static void main(String[] arg) {
        new stormConsumer(KafkaProperties.topic).start();
    }

    public Queue<String> getQueue()
    {
        return queue;
    }
}