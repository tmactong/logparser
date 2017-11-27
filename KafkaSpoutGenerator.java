package logparser.spout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaSpoutGenerator extends KafkaSpout {
    private static final String bootstrapServers = "10.206.28.74:9092,10.206.28.8:9092,10.206.28.81:9092,10.206.28.89:9092,10.206.3.121:9092,10.206.3.127:9092,10.206.32.104:9092,10.206.32.107:9092";
    private static final String kafkaTopic = "log-collector";

    public KafkaSpoutGenerator(KafkaSpoutConfig kafkaConfig) {
        super(kafkaConfig);
    }

    public KafkaSpoutGenerator(){
        this(KafkaSpoutGenerator.getKafkaSpoutConfig());
    }

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig() {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>((r) -> new Values(r.value()),new Fields("value"));
        return KafkaSpoutConfig.builder(bootstrapServers, kafkaTopic)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutLogParser")
            .setRecordTranslator(trans)
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }
}
