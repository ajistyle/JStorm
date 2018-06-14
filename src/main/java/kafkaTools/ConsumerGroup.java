package kafkaTools;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/3/26
 * @since 1.0.0
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ConsumerGroup {
    private final String brokers;
    private final String groupId;
    private final String topic;
    private final int consumerNumber;


    private List<ConsumerThread> consumerThreadList = new ArrayList<ConsumerThread>();

    public ConsumerGroup(String brokers, String groupId, String topic, int consumerNumber){
        this.groupId = groupId;
        this.topic = topic;
        this.brokers = brokers;
        this.consumerNumber = consumerNumber;
        for(int i = 0; i< consumerNumber;i++){
            ConsumerThread consumerThread = new ConsumerThread(brokers,groupId,topic);
            consumerThreadList.add(consumerThread);
        }
    }

    public void start(){
        for (ConsumerThread item : consumerThreadList){
            Thread thread = new Thread(item);
            thread.start();
        }
    }

}