import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class CountBolt extends BaseRichBolt {
    private HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
    private OutputCollector collector;
    //此方法和Spout中的open方法类似，为Bolt提供了OutputCollector，用来从Bolt中发送Tuple。执行在execute方法之前
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    //对于Tuple的处理都可以放到此方法中进行。具体的发送也是通过emit方法来完成的
    public void execute(Tuple tuple) {

        //你处理的每条消息要么是确认的（注：collector.ack()）要么是失败的（注：collector.fail()）。
        // Storm 使用内存跟踪每个元组，所以如果你不调用这两个方法，该任务最终将耗尽内存
        try {
            //从tuple中读取单词
            String word = tuple.getStringByField("word");

            //计数
            int num;
            if (wordMap.containsKey(word)) {
                num = wordMap.get(word);
            } else {
                num = 0;
            }
            wordMap.put(word, 1 + num);

            //输出展示
            Set<String> keys = wordMap.keySet();
            for (String key : keys)
            {
                System.out.print(key + ":" + wordMap.get(key) + ",");
            }
            System.out.println();

            collector.ack(tuple);
        }catch (Exception e)
        {
            collector.fail(tuple);
        }
    }

    //用于声明当前Bolt发送的Tuple中包含的字段
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}