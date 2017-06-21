package org.apache.storm.kafka.spout.test.trident;

import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import backtype.storm.LocalCluster;


import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;


/**
 * Test for Topology with Trident Kafka Spout
 */
public class KafkaTridentSpoutTopology {

    private static final String[] TOPICS = new String[]{"test"};

    /**
     * To run this topology ensure you have a kafka broker running and provide connection string to broker as argument.
     * Create a topic test with command line,
     * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
     *
     * run this program and run the kafka consumer:
     * kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
     *
     * you should see the messages flowing through.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new KafkaTridentSpoutTopology().runMain();
    }

    protected void runMain() throws Exception {
        submitTopologyLocalCluster(getTopologyKafkaSpout(), getConfig());
    }

    protected void submitTopologyLocalCluster(StormTopology topology, Config config) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        stopWaitingForInput();
    }

    protected  Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTopologyKafkaSpout() {
        TridentTopology topology = new TridentTopology();
        topology.newStream("kafka_spout", new KafkaTridentSpoutOpaque<>(getKafkaSpoutConfig()))
            .each(new Fields("topic", "partition", "offset", "key", "value"),new KafkaTridentTestFunction(),new Fields());
        return topology.build();
    }

    protected KafkaSpoutConfig<String,String> getKafkaSpoutConfig() {

        return KafkaSpoutConfig.builder("127.0.0.1:9092", TOPICS)
            .setGroupId("kafkaSpoutTestGroup")
            .setRetry(getRetryService())
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(
            KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
            KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval
                .seconds(10));
    }

    protected void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
