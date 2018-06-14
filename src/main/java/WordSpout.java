import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;


public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] words = {"hello","world","storm","study"};//单词池
    private int index = 0;


    //当一个Task被初始化的时候会调用此open方法。一般都会在此方法中对发送Tuple的对象SpoutOutputCollector和配置对象TopologyContext初始化
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    //发射一个Tuple到Topology
    public void nextTuple() {
        this.collector.emit(new Values(this.words[index]));
        index++;
        if(index>=words.length){
            index = 0;
        }

        //等待5000ms
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));//声明当前Spout的Tuple发送流
    }
}