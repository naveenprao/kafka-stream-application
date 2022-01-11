package wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class StreamsWordCount {
    public static void main(String[] args) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // 1. Stream from kafka
        KStream<String, String> wordCountInput = streamsBuilder.stream("streams-plaintext-input", Consumed.with(stringSerde, stringSerde));
        // 2. map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(values -> values.toLowerCase())
                // 3. flatmap values split by space
                .flatMapValues(lowercasetext -> Arrays.asList(lowercasetext.split(" ")))
                // 4. select key to apply a key
                .selectKey((ignoredkey, word) -> word)
                // 5. group by key before aggreation
                .groupBy((key, value) -> value)
                // 6. count occurances
                .count(Named.as("Counts"));
        // 7. write back to kafka
        wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        log.info(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
