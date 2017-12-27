package logparser.topology;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.lang.Integer;
import java.lang.Float;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;

public class WindowGroupedBolt extends BaseWindowedBolt {

    protected static final Logger LOG = getLogger();
    private OutputCollector collector;
    final float standardRatio = 1.1f;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    static class State {
        int count = 0;
        long cpu_sum = 0;
        long memory_sum = 0;
        long cpu_avg = 0;
        long memory_avg = 0;
        Map<Integer, List<Integer>> PartitionResource = new HashMap<Integer, List<Integer>>();
        //Map<Integer, Object> PartitionResource = new HashMap<Integer, Object>();
        List<Integer> abnormalPartitions = new ArrayList<Integer>();
    }

    public static Logger getLogger(){
        Logger Log = Logger.getLogger(WindowGroupedBolt.class.getName());
        FileHandler fileHandler;
        try {
            SimpleFormatter simple = new SimpleFormatter();
            fileHandler = new FileHandler("/apsarapangu/disk1/storm/userlogs/window.log");
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
    public void execute(TupleWindow inputWindow) {
        Map<String, State> TaskResource = new HashMap<String, State>();
        List<Tuple> tuplesInWindow = inputWindow.get();
        if (tuplesInWindow.size() > 0) {
            for (Tuple tuple : tuplesInWindow){
                String TaskId = tuple.getString(0);
                int PartitionId = tuple.getInteger(1);
                int cpu = tuple.getInteger(2);
                int memory = tuple.getInteger(3);
                if(!TaskResource.containsKey(TaskId)) {
                    TaskResource.put(TaskId, new State());
                }
                State TaskState =  TaskResource.get(TaskId);
                TaskState.count += 1;
                TaskState.cpu_sum += cpu;
                TaskState.memory_sum += memory;
                //TaskState.PartitionResource.put(PartitionId, new int[] {cpu, memory});
                TaskState.PartitionResource.put(PartitionId, new ArrayList<Integer>(Arrays.asList(cpu, memory)));
            }
            for (Map.Entry<String, State> Task : TaskResource.entrySet()) {
                State taskState = Task.getValue();
                taskState.cpu_avg = taskState.cpu_sum / taskState.count;
                taskState.memory_avg = taskState.memory_sum / taskState.count;
                for (Map.Entry<Integer, List<Integer>> Partition : taskState.PartitionResource.entrySet()) {
                    List<Integer> partition = Partition.getValue();
                    float cpu_percent = ((float) partition.get(0)) / taskState.cpu_avg;
                    float memory_percent = ((float) partition.get(1)) / taskState.memory_avg;
                    if(Float.compare(cpu_percent, standardRatio) > 0) {
                        taskState.abnormalPartitions.add(Partition.getKey());
                    }
                }
                LOG.info(()-> String.format("Task: %s, Count: %d, Cpu sum: %d, Memory sum: %d, Cpu avg: %d, Memory avg %d, Map: %s, abnormal partitions: %s",  Task.getKey(), taskState.count, taskState.cpu_sum, taskState.memory_sum, taskState.cpu_avg, taskState.memory_avg, taskState.PartitionResource.toString(), taskState.abnormalPartitions.toString()));
                collector.emit(new Values(Task.getKey(), taskState.cpu_sum, taskState.memory_sum, taskState.cpu_avg, taskState.memory_avg));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("task", "cpu_sum", "mem_sum", "cpu_avg", "mem_avg"));
    }
}
