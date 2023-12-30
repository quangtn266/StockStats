package com.github.quangtn.kafka.streams;


import com.github.quangtn.kafka.streams.serde.JsonDeserializer;
import com.github.quangtn.kafka.streams.serde.JsonSerializer;
import com.github.quangtn.kafka.streams.serde.WrapperSerde;
import com.github.quangtn.kafka.streams.model.Trade;
import com.github.quangtn.kafka.streams.model.TradeStats;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;


import java.time.Duration;
import java.util.Properties;

/*
* Input is a stream of trades
* Output is 2 streams: One wiht minimum and avg "ASK" price for every 10 second
* window.
* Another with the top 3 stocks wiht lowest minimum ask every minute
* */
public class StockStatsExample {

    public static void main(String[] args) throws Exception {
        // Properties props;
/*        if (args.length==1)
            props = LoadConfigs.loadConfig(args[0]);
        else
            props = LoadConfigs.loadConfig();*/

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());


         /*
         Setting offset reset to earliest so that we can re-run the demo code with the same pre-load
         data.
         Note: To re-run the demo, you need to use the offset reset tool:
         https://cwiki.apache.org/confulence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Time interval, in millisecond, for our aggregation window.
        long windowSize = 5000;

        // Creating an AdminClient and checking the number of brokers in the cluster, so We will
        // know how many replica we need.

        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();

        if (clusterSize<3)
            props.put("replication.factor", clusterSize);
        else
            props.put("replication.factor", 3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

        KStream<Windowed<String>, TradeStats> stats = source
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(windowSize)).advanceBy(Duration.ofSeconds(1)))
                .<TradeStats>aggregate(() -> new TradeStats(),(k, v, tradestats) -> tradestats.add(v),
                    Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")
                        .withValueSerde(new TradeStatsSerde()))
                .toStream()
                .mapValues((trade) -> trade.computeAvgPrice());

        stats.to("stockstats-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize)));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println(topology.describe());

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully clost Kafka Streams.
        Runtime.getRuntime().addShutdownHook(new Thread(streams:: close));

    }

    static public final class TradeSerde extends Serdes.WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
        }
    }
    static  public final class TradeStatsSerde extends WrapperSerde<TradeStats> {

            public TradeStatsSerde() {
                super(new JsonSerializer<TradeStats>(), new JsonDeserializer<TradeStats>(TradeStats.class));
            }
        }
}
