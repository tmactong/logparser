package logparser.topology;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.hbase.bolt.HBaseBolt;

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
        HBaseLogParserMapper mapper = new HBaseLogParserMapper();
        tp.setSpout("KafkaSpout", new KafkaSpoutGenerator(), 10);
        tp.setBolt("LogParserBolt", new LogParserBolt(), 10).localOrShuffleGrouping("KafkaSpout");
        tp.setBolt("WindowGroupedBolt", new WindowGroupedBolt().withWindow(new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS)), 10).localOrShuffleGrouping("LogParserBolt");
        tp.setBolt("Write2HBaseBolt", new HBaseBolt("AbnormalResource", mapper).withConfigKey("HBaseConfig"), 10).localOrShuffleGrouping("WindowGroupedBolt");
        return tp.createTopology();
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(10);
        Map HBaseConfig = new HashMap<String, String>();
        HBaseConfig.put("hbase.rootdir", "hdfs://r61d09363.cm10/hbase");
        HBaseConfig.put("hbase.zookeeper.quorum", "r61b13319.cm10.tbsite.net,r61b07320.cm10.tbsite.net,r61b04224.cm10.tbsite.net");
        config.put("HBaseConfig", HBaseConfig);
        return config;
    }
}
