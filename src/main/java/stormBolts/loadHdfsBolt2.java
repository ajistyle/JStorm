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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 〈功能简述〉
 * 〈〉
 *
 * @author zhuji
 * @create 2018/6/11
 * @since 1.0.0
 */
public class loadHdfsBolt2 implements IRichBolt {

    private static final long serialVersionUID = -4880963572379636724L;
    private OutputCollector collector;
    private FileSystem fileSystem;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        try
        {
            //"VehicleNum","PlateColor","RecordTime","VehicleInfo"
            /*  String    */
//            String line0 = tuple.getString(0).trim();
//            String line1 = tuple.getString(1).trim();
//            String line2 = tuple.getString(2).trim();
//            String line3 = tuple.getString(3).trim();
//
//            if(!line0.isEmpty() && !line1.isEmpty() && !line2.isEmpty())
//            {
//                System.err.println("bolt_line[0]:"+ line0);
//                System.err.println("bolt_line[1]:"+ line1);
//                System.err.println("bolt_line[2]:"+ line2);
//                System.err.println("bolt_line[3]:"+ line3);
//            }
//
//            Configuration conf=new Configuration();
//            conf.set("fs.defaultFS","hdfs://Node1:9000");
//            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//            conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//            conf.set("dfs.support.append", "true");
//            conf .set("dfs.client.block.write.replace-datanode-on-failure.policy" ,"NEVER" );
//            conf .set("dfs.client.block.write.replace-datanode-on-failure.enable" ,"true" );
//            fileSystem = FileSystem.get(conf);
//            byte[] buff=(line3+"\n").getBytes();
//
//            Date yMd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(line2);
//            SimpleDateFormat sdy = new SimpleDateFormat("yyyy");
//            SimpleDateFormat sdM = new SimpleDateFormat("MM");
//            SimpleDateFormat sdd = new SimpleDateFormat("dd");
//            SimpleDateFormat sHH = new SimpleDateFormat("HH");
//            SimpleDateFormat smm = new SimpleDateFormat("mm");
//            SimpleDateFormat sss = new SimpleDateFormat("ss");
//
//            String stry = sdy.format(yMd);
//            String strM = sdM.format(yMd);
//            String strd = sdd.format(yMd);
//
//            String strHH = sHH.format(yMd);
//            String strmm = smm.format(yMd);

            /*  Json    */
            String strVehicleNum = tuple.getString(0).trim();
            String strRecordTime = tuple.getString(1).trim();
            String strVehicleInfo = tuple.getString(2).trim();
//            System.err.println("strRecordTime: "+strRecordTime+"; strVehicleInfo:"+strVehicleInfo);

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
            String strPath = "hdfs://Node1:9000/storm/testJson2/"+stry+"/"+strM+"/"+strd+"/"+strVehicleNum+"/"+strmm+".txt";
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
        }
        catch (Exception e)
        {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    public void cleanup() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}