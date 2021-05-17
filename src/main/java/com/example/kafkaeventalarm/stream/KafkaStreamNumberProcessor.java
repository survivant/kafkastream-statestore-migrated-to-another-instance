package com.example.kafkaeventalarm.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import com.example.kafkaeventalarm.model.Order;
import com.example.kafkaeventalarm.stream.serdes.SerdeFactory;

@Service
public class KafkaStreamNumberProcessor {
    private final Logger logger = LoggerFactory.getLogger(KafkaStreamNumberProcessor.class);

    @Value("${number.topic.name}")
    private String inputTopic;

    @Value("${number.stream.output.name}")
    private String numberStreamOutput;

    @Qualifier("NumberStreamProcessor")
    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private KafkaStreams streams;

    @Autowired
    public void process(@Qualifier("NumberStreamProcessor") StreamsBuilder builder) {

        Map<String, Object> serdeProps = new HashMap<>();
        Serde<Order> orderSerde = SerdeFactory.createSerde(Order.class, serdeProps);

        // Serializers/deserializers (serde) for String and Long types
        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic, where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        KStream<String, Order> textLines = builder.stream(inputTopic, Consumed.with(stringSerde, orderSerde));

        //textLines.print(Printed.toSysOut());

        KTable<String, Long> wordCounts = textLines
                .filter((key, value) -> {System.out.println("    KafkaStreamNumberProcessor-Process Key=" + key + "  value=" + value); return true;})
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toString().split("\\W+")))
                // We use `groupBy` to ensure the words are available as message keys
                .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
                // Count the occurrences of each word (message key).
                .count(Materialized.as("counts"));

        // Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        wordCounts.toStream().to(numberStreamOutput, Produced.with(stringSerde, longSerde));

        streams = new KafkaStreams(builder.build(), streamsBuilderFactoryBean.getStreamsConfiguration());
        // Clean local store between runs
        streams.cleanUp();
        streams.start();
    }

    @PreDestroy
    public void destroy() {
        streams.close();
    }

}