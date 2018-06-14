package stormTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import stormBolts.*;
import stormSpouts.extractkafkaSpout;
import stormSpouts.extractkafkaSpout2;

import java.util.Arrays;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/6/11
 * @since 1.0.0
 */
public class dataEtlTopology {

    public static void main(String[] args) {

        // Configure Kafka
        String zks = "Node1:2181,Node2:2181,Node3:2181";
        String topic = KafkaProperties.topic;
        String zkRoot = "/stormKafka";  //进度信息记录于zookeeper的哪个路径下
        //读取的status会被存在，/stormKafka/kafkaSpout_Read 下面，所以id类似consumer group
        String id = "kafkaSpout_Read";  //进度记录的id，想要一个新的Spout读取之前的记录，应把它的id设为跟之前的一样。
        BrokerHosts brokerHosts = new ZkHosts(zks);

        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//        spoutConf.forceFromStart = false;
        spoutConf.zkServers = Arrays.asList("Node1", "Node2", "Node3");
        spoutConf.zkPort = 2181;
        spoutConf.stateUpdateIntervalMs=2000;
        spoutConf.startOffsetTime=-1; //从最新的开始消费


        TopologyBuilder builder = new TopologyBuilder();
        //字符串按逗号分割
        builder.setSpout("extractkafkaSpout", new extractkafkaSpout(), 1);
        builder.setBolt("loadHdfsBolt", new loadHdfsBolt(), 1).shuffleGrouping("extractkafkaSpout");

//        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 3);
//        builder.setBolt("JsonBolt", new JsonBolt(), 3).shuffleGrouping("kafka-reader");
//        builder.setBolt("JsonToHdfsBolt", new JsonToHdfsBolt(), 1).shuffleGrouping("JsonBolt");

        Config config = new Config();
        config.setDebug(false);

        if(args.length>0)
        {
            try {
               //工作节点数
                config.setNumWorkers(2);
                //一次读取kafka10000的数据
                config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);

                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            }
            catch (AlreadyAliveException e)
            {
                e.printStackTrace();
            }
            catch (InvalidTopologyException e)
            {
                e.printStackTrace();
            }
        }
        else
        {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("myTopology",config,builder.createTopology());
        }

    }
}