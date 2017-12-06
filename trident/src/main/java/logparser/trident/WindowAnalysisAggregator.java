package logparser.trident;

import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;

public class WindowAnalysisAggregator extends BaseAggregator<WindowAnalysisAggregator.State> {

    protected static final Logger LOG = getLogger();

    static class State {
        long cpu_sum = 0;
        long memory_sum = 0;
    }
    
    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
        int PartitionId = tuple.getInteger(1);
        int cpu = tuple.getInteger(2);
        int memory = tuple.getInteger(3);
        LOG.info(()-> String.format("New tuple coming. TaskId:%s, Pid:%d,CPU:%d,Memory:%d", tuple.getString(0), PartitionId, cpu, memory));
        state.cpu_sum += cpu;
        state.memory_sum += memory;
    }

    public static Logger getLogger(){
        Logger Log = Logger.getLogger(WindowAnalysisAggregator.class.getName());
        FileHandler fileHandler;
        try {
            SimpleFormatter simple = new SimpleFormatter();
            fileHandler = new FileHandler("/apsarapangu/disk1/storm/userlogs/aggregator.log");
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
    public void complete(State state, TridentCollector collector) {
        LOG.info(()-> String.format("Cpu sum: %d, Memory sum: %d", state.cpu_sum, state.memory_sum));
        collector.emit(new Values(state.cpu_sum, state.memory_sum));
    }
    
}
