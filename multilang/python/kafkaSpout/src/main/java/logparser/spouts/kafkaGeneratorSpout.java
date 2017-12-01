package logparser.spouts;

import storm.kafka.SpoutConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

public class kafkaGeneratorSpout extends KafkaSpout {

	public kafkaGeneratorSpout(SpoutConfig spoutConf) {
		super(spoutConf);
	}

	public kafkaGeneratorSpout() {
		this(kafkaGeneratorSpout.defaultSpoutConfig());
	}

	public static SpoutConfig defaultSpoutConfig() {
		ZkHosts hosts = new ZkHosts("10.206.28.74:2181,10.206.28.8:2181,10.206.28.81:2181", "/brokers");
		SpoutConfig spoutConf = new SpoutConfig(hosts, "log-collector", "/kafka_storm", "log_parser_offset");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = false;
		return spoutConf;
	}
}
