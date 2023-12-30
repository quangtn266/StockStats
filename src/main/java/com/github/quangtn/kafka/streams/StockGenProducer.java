package com.github.quangtn.kafka.streams;

// import com.github.quangtn.kafka.streams.Constants;
import com.github.quangtn.kafka.streams.serde.JsonSerializer;
import com.github.quangtn.kafka.streams.model.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class StockGenProducer {

    public static KafkaProducer<String, Trade> producer = null;

    public static void main(String[] args) throws Exception {

        System.out.println(("Press CTRL-C to stop generating data"));

        // add shut-down hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run () {
                System.out.println("Shutting down");
                if (producer != null)
                    producer.close();
                            }
            }
        );

        JsonSerializer<Trade> tradeSerializer = new JsonSerializer<>();

        // Configuring producer
        //Properties props;
        Properties props = new Properties();
/*        if (args.length == 1)
            props = LoadConfigs.loadConfig(args[0]);
        else
            props = LoadConfigs.loadConfig();*/

        // Kafka bootstrap-server.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer acks
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", tradeSerializer.getClass().getName());

        // Starting producer
        producer = new KafkaProducer<>(props);

        // initialize
        Random random = new Random();
        long iter = 0;

        Map<String, String> prices = new HashMap<>();

        for (String ticker: Constants.TICKERS)
            prices.put(ticker, String.valueOf(Constants.START_PRICE));

        // Starting generating events, stop when CTRL-C
        while (true) {
            iter++;
            for (String ticker: Constants.TICKERS) {
                // random var from lognormal dist with stddev=0.25 and mean=1
                double log = random.nextGaussian() * 0.25 + 1;
                int size = random.nextInt(100);
                int price = Integer.parseInt(prices.get(ticker));

                // flunctuate price sometimes.
                if (iter % 10 == 0) {
                    price = price + random.nextInt(Constants.MAX_PRICE_CHANGE * 2) - Constants.MAX_PRICE_CHANGE;
                    prices.put(ticker, String.valueOf(price));
                }

                Trade trade = new Trade("ASK", ticker, (price + log), size);
                // Note that we are using ticker as the key - so all for same stock will be
                // in the same partition.
                ProducerRecord<String ,Trade> record = new ProducerRecord<>(Constants.STOCK_TOPIC, ticker, trade);

                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing events");
                        e.printStackTrace();
                    }
                        }
                        );

                // sleep a bit, otherwise it is frying my machine
                Thread.sleep(Constants.DELAY);
            }
        }
    }
}
