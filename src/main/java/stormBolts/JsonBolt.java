package stormBolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONValue;

import java.util.Map;

public class JsonBolt implements IRichBolt {

    private static final long serialVersionUID = 726423390686744732L;
    private Fields fields;
    private OutputCollector collector;

    public JsonBolt() {
//        this.fields = new Fields("VehicleNum", "PlateColor", "RecordTime",
//                "VehicleInfo");
        this.fields = new Fields("VehicleNum", "PlateColor", "RecordTime");
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            String DataJson = tuple.getString(0);
//            System.err.println("DataJson：" + DataJson);

            Map<String, Object> map = (Map<String, Object>)JSONValue.parse(DataJson);
            String strRecordtime = map.get(this.fields.get(2)).toString();
//            System.err.println("strRecordtime：" + strRecordtime);

//            Values values = new Values();
//            for (int i = 0, size = this.fields.size(); i < size; i++) {
//                values.add(map.get(this.fields.get(i)));
//            }
            collector.emit(tuple,new Values(strRecordtime,DataJson));
            this.collector.ack(tuple);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            collector.fail(tuple);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("RecordTime","VehicleInfo"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
