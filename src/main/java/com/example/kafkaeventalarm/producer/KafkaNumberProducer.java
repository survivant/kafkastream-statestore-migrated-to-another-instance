package com.example.kafkaeventalarm.producer;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.kafkaeventalarm.Constants;
import com.example.kafkaeventalarm.model.Order;

@Component
public class KafkaNumberProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaNumberProducer.class);

    @Value("${number.topic.name}")
    private String inputTopic;

    private long counter = 0;

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @Scheduled(fixedRate = 3000)
    public void produce() {
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .product(Long.toString(counter++))
                .status("NEW")
                .orderTimestamp(Instant.now().toString())
                .build();
        logger.debug(String.format("#### -> Producing Order -> %s", order));
        this.kafkaTemplate.send(inputTopic, order.getOrderId(), order);
    }

}