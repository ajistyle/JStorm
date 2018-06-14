package stormBolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class JsonToHdfsBolt implements IRichBolt {
    private static final long serialVersionUID = -1234287212707023988L;
    private OutputCollector collector;
    private FileSystem fileSystem;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        System.err.println("JsonToHdfsBolt--prepare");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
//        System.err.println("JsonToHdfsBolt--execute");
        try {
            String strRecordTime = tuple.getString(0).trim();
            String strVehicleInfo = tuple.getString(1).trim();
//            System.err.println("RecordTime: "+strRecordTime);
//            System.err.println("VehicleInfo: "+strVehicleInfo);

            Configuration conf=new Configuration();
            conf.set("fs.defaultFS","hdfs://Node1:9000");
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
            conf.set("dfs.support.append", "true");
            conf .set("dfs.client.block.write.replace-datanode-on-failure.policy" ,"NEVER" );
            conf .set("dfs.client.block.write.replace-datanode-on-failure.enable" ,"true" );
            fileSystem = FileSystem.get(conf);
            byte[] buff=(strVehicleInfo+"\n").getBytes();

            Date yMd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strRecordTime.replace("T"," "));
            SimpleDateFormat sdy = new SimpleDateFormat("yyyy");
            SimpleDateFormat sdM = new SimpleDateFormat("MM");
            SimpleDateFormat sdd = new SimpleDateFormat("dd");
            SimpleDateFormat sHH = new SimpleDateFormat("HH");
            SimpleDateFormat smm = new SimpleDateFormat("mm");
            SimpleDateFormat sss = new SimpleDateFormat("ss");

            String stry = sdy.format(yMd);
            String strM = sdM.format(yMd);
            String strd = sdd.format(yMd);
            String strHH = sHH.format(yMd);
            String strmm = smm.format(yMd);

            //上传的位置
            String strPath = "hdfs://Node1:9000/storm/"+stry+"/"+strM+"/"+strd+"/"+strmm+".txt";
            Path outPath = new Path(strPath);
            FSDataOutputStream outStream = null;

            if (!fileSystem.exists(outPath)) {
                fileSystem = FileSystem.get(conf);
                outStream = fileSystem.create(outPath,false);
            }
            else
            {
                //新建输出流
                outStream = fileSystem.append(outPath);
            }
            //上传文件
            outStream.write(buff,0,buff.length);
            outStream.close();
            this.collector.ack(tuple);
            System.err.println("ok!");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {
        System.err.println("JsonToHdfsBolt--cleanup");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        System.err.println("JsonToHdfsBolt--declareOutputFields");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
//        System.err.println("JsonToHdfsBolt--getComponentConfiguration");
        return null;
    }
}
