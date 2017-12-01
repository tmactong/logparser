package logparser.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class LogParserTopology {

    public static void main(String[] args) throws Exception {
        new LogParserTopology().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        Config tpConf = getConfig();
        String topologyName = args[0];
        StormSubmitter.submitTopology(topologyName, tpConf, getTopology());
    }

    protected StormTopology getTopology() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("KafkaSpout", new KafkaSpoutGenerator(), 10);
        tp.setBolt("LogParserBolt", new LogParserBolt(), 10).localOrShuffleGrouping("KafkaSpout");
        return tp.createTopology();
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(10);
        return config;
    }
}
