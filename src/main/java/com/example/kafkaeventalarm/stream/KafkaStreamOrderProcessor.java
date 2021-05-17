package com.example.kafkaeventalarm.stream;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import com.example.kafkaeventalarm.model.Order;
import com.example.kafkaeventalarm.stream.serdes.SerdeFactory;

@Service
public class KafkaStreamOrderProcessor {
    private final Logger logger = LoggerFactory.getLogger(KafkaStreamOrderProcessor.class);

    @Value("${order.topic.name}")
    private String inputTopic;

    @Value("${order.stream.output.name}")
    private String orderStreamOutput;

    private KafkaStreams streams;

    @Qualifier("OrderStreamProcessor")
    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    public void process(@Qualifier("OrderStreamProcessor") StreamsBuilder builder) {

        Map<String, Object> serdeProps = new HashMap<>();
        Serde<Order> orderSerde = SerdeFactory.createSerde(Order.class, serdeProps);

        // Serializers/deserializers (serde) for String and Long types
        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic where message values
        KStream<String, Order> textLines = builder.stream(inputTopic, Consumed.with(stringSerde, orderSerde));

        textLines
                .filter((key, value) -> {System.out.println("    KafkaStreamOrderProcessor Key=" + key + "  value=" + value); return true;})
                .selectKey((key, value) -> value.getStatus())
                .groupBy((s, order) -> order.getStatus(), Grouped.with(stringSerde, orderSerde))
                .count(Materialized.as(orderStreamOutput));

        streams = new KafkaStreams(builder.build(), streamsBuilderFactoryBean.getStreamsConfiguration());
        // Clean local store between runs
        streams.cleanUp();
        streams.start();

    }

    public ReadOnlyKeyValueStore<String, Long> getInteractiveQueryCount() {
        return streams.store(orderStreamOutput, QueryableStoreTypes.keyValueStore());
    }

    @PreDestroy
    public void destroy() {
        streams.close();
    }
}