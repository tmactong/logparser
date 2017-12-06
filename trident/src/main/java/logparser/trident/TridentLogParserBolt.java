package logparser.trident;

import java.util.Map;
import java.lang.Integer;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;
import org.apache.storm.tuple.Values;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//Temporary Import Modules


public class TridentLogParserBolt extends BaseFunction {
    protected static final Logger LOG = getLogger();
    
    //@Override
    //public void prepare(Map<String, Object> conf, TridentOperationContext context) {
    //}

    public static Logger getLogger(){
        //Logger Log = LoggerFactory.getLogger(LogParserBolt.class);
        Logger Log = Logger.getLogger(TridentLogParserBolt.class.getName());
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
    public void execute(TridentTuple input, TridentCollector collector){
        String[] array = input.getString(0).split("\\+");
        LOG.info(()-> String.format("TaskId: %s, Pid: %s, CPU: %s, Memory: %s", array[0], array[1], array[2], array[3]));
        collector.emit(new Values(array[0], Integer.parseInt(array[1]), Integer.parseInt(array[2]), Integer.parseInt(array[3])));
    }
}
