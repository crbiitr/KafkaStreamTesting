package com.learning.KafkaStreamTesting.wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by chetan on 13/12/19.
 */
@Slf4j
@EnableKafkaStreams
public class WordCount {
//    @Autowired private KafkaProperties kafkaProperties;

    //    kafkaProperties.getBootstrapServers()

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountStream = builder.stream("word-count-input");

        KTable<String, Long> wordCountTable = wordCountStream
                .mapValues(line -> line.toLowerCase())
                .flatMapValues(lowerCaseTestLine -> Arrays.asList(lowerCaseTestLine.split(" ")))
                .selectKey((ignoreKey,word) -> word)
                // Group by key before aggregation
                .groupByKey()
                // Count occurrences
                .count(Materialized.as("Counts"));

        System.out.println("wordCountTable===>");
        System.out.println(wordCountTable.toString()); //foreach((w, c) -> System.out.println("word: " + w + " -> " + c));

        wordCountTable.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

//        wordCountTable.toStream(Serdes.String(),Serdes.String(),"output");
//        combinedDocuments.toStream().to("streams-json-output", Produced.with(Serdes.String(), new JsonSerde<>(Test.class)));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),properties);
        kafkaStreams.start();

        System.out.println("KafkaStream===>");
        // Print the topology
        System.out.println(kafkaStreams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


    }

    @Bean
    public Topology kafkaStreamTopology() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // streamsBuilder.stream("some_topic") etc ...

        return streamsBuilder.build();
    }
}
