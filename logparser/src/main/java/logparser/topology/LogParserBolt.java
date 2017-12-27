package logparser.topology;

import java.util.Map;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//Temporary Import Modules


public class LogParserBolt extends BaseRichBolt {
    protected static final Logger LOG = getLogger();
    private OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public static Logger getLogger(){
        //Logger Log = LoggerFactory.getLogger(LogParserBolt.class);
        Logger Log = Logger.getLogger(LogParserBolt.class.getName());
        FileHandler fileHandler;
        try {
            SimpleFormatter simple = new SimpleFormatter();
            fileHandler = new FileHandler("/apsarapangu/disk1/storm/userlogs/data.log");
            fileHandler.setFormatter(simple);
            Log.addHandler(fileHandler);
        } catch (IOException ex) {
            Log.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (SecurityException ex) {
            Log.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return Log;
    }
    
    @Override
    public void execute(Tuple input) {
        String[] array = input.getString(0).split("\\+");
        LOG.info(()-> String.format("TaskId: %s, Pid: %s, CPU: %s, Memory: %s", array[0], array[1], array[2], array[3]));
        _collector.emit(new Values(array[0], Integer.parseInt(array[1]), Integer.parseInt(array[2]), Integer.parseInt(array[3])));
        _collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("TaskId", "PartitionId", "CPU", "Memory"));
    }
}
