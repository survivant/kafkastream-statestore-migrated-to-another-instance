package com.example.kafkaeventalarm.config;

import com.example.kafkaeventalarm.model.Order;
import com.example.kafkaeventalarm.model.Return;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${order.topic.group.id}")
    private String orderGroupId;

    @Value(value = "${return.topic.group.id}")
    private String returnGroupId;

    @Value(value = "${number.topic.group.id}")
    private String numberGroupId;

    public ConsumerFactory<String, Order> orderConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "OrderConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, orderGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory());
        return factory;
    }

    public ConsumerFactory<String, Return> returnConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ReturnConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, returnGroupId);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Return> returnKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Return> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(returnConsumerFactory());
        return factory;
    }

    public ConsumerFactory<String, Return> numberConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "NumberConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, numberGroupId);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Return> numberKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Return> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(numberConsumerFactory());
        return factory;
    }
}
