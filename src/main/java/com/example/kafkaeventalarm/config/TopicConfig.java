package com.example.kafkaeventalarm.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TopicConfig {
    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${order.topic.name}")
    private String orderTopicName;

    @Value(value = "${order.stream.output.name}")
    private String orderStreamOutputName;

    @Value(value = "${return.topic.name}")
    private String returnTopicName;

    @Value(value = "${number.topic.name}")
    private String numberTopicName;

    @Value("${order.window.topic.name}")
    private String inputTopicWindow;

    @Value("${order.stream.window.output.name}")
    private String orderStreamWindowOutput;

    @Value("${number.stream.output.name}")
    private String numberStreamOutput;


    @Bean
    public NewTopic orderTopic() {
        return TopicBuilder.name(orderTopicName)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic returnTopic() {
        return TopicBuilder.name(returnTopicName)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderStreamOutputName() {
        return TopicBuilder.name(orderStreamOutputName)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic streamNumberProcessorTopic() {
        return TopicBuilder.name(numberStreamOutput)
                .partitions(1)
                .replicas(1)
                //.config("retention.ms", "60000")
                //.config("segment.ms", "60000")
                .build();
    }

    @Bean
    public NewTopic orderStreamWindowOutputTopic() {
        return TopicBuilder.name(orderStreamWindowOutput)
                .partitions(1)
                .replicas(1)
                //.config("retention.ms", "60000")
                //.config("segment.ms", "60000")
                .build();
    }

    @Bean
    public NewTopic numberTopicName() {
        return TopicBuilder.name(numberTopicName)
                .partitions(1)
                .replicas(1)
               // .config("retention.ms", "60000")
                //.config("segment.ms", "60000")
                .build();
    }

    @Bean
    public NewTopic orderStreamProcessorWindowTopic() {
        return TopicBuilder.name(inputTopicWindow)
                .partitions(1)
                .replicas(1)
                //.config("retention.ms", "60000")
                //config("segment.ms", "60000")
                .build();
    }


    //If not using spring boot

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }
}
