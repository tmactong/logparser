package logparser.trident;

import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.trident.operation.impl.GroupedAggregator;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;

public class TridentLogParserTopology {

    public static void main(String[] args) throws Exception {
        new TridentLogParserTopology().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        Config tpConf = getConfig();
        String topologyName = args[0];
        StormSubmitter.submitTopology(topologyName, tpConf, newTopology());
    }

    protected StormTopology newTopology() {
        final TridentTopology tridentTp = new TridentTopology();
        final Stream KafkaStream = tridentTp.newStream("TridentKafkaSpout", new TridentKafkaSpout());
        KafkaStream
            .each(KafkaStream.getOutputFields(), new TridentLogParserBolt(), new Fields("TaskId", "PartitionId", "CPU", "Memory"))
            //.window(SlidingDurationWindow.of(new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS)), new InMemoryWindowsStoreFactory(), new Fields("TaskId", "CPU", "Memory"), new GroupedAggregator(new WindowAnalysisAggregator(), new Fields("TaskId"), new Fields("TaskId", "CPU", "Memory"), 10), new Fields("Sum"))
            .window(SlidingDurationWindow.of(new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS)), new InMemoryWindowsStoreFactory(), new Fields("TaskId", "PartitionId", "CPU", "Memory"), new WindowAnalysisAggregator(), new Fields("Sum"))
            .parallelismHint(1);
        return tridentTp.build();
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(1);
        return config;
    }
}
