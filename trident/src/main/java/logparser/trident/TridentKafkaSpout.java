package logparser.trident;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

//https://github.com/apache/storm/blob/master/examples/storm-kafka-client-examples/src/main/java/org/apache/storm/kafka/trident/TridentKafkaClientTopologyNamedTopics.java

public class TridentKafkaSpout extends KafkaTridentSpoutOpaque {
    private static final String bootstrapServers = "10.206.28.74:9092,10.206.28.8:9092,10.206.28.81:9092,10.206.28.89:9092,10.206.3.121:9092,10.206.3.127:9092,10.206.32.104:9092,10.206.32.107:9092";
    private static final String kafkaTopic = "log-collector";

    public TridentKafkaSpout(KafkaSpoutConfig kafkaConfig) {
        super(kafkaConfig);
    }

    public TridentKafkaSpout() {
        this(TridentKafkaSpout.newKafkaSpoutConfig());
    }

    private static final Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new JustValueFunc();

    private static class JustValueFunc implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {

        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value());
        }
    }

    protected static KafkaSpoutConfig<String, String> newKafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(bootstrapServers, kafkaTopic)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutLogParserGroup_" + System.nanoTime())
            .setRecordTranslator(JUST_VALUE_FUNC, new Fields("Log"))
            .setRetry(newRetryService())
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(LATEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }

    protected static KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(
                new TimeInterval(500L, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
                TimeInterval.seconds(10));
    }

}
