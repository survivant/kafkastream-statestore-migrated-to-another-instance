package com.example.kafkaeventalarm.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.kafkaeventalarm.Constants;
import com.example.kafkaeventalarm.model.Order;

@Service
public class OrderNumberConsumer {
    private final Logger logger = LoggerFactory.getLogger(OrderNumberConsumer.class);

    @KafkaListener(topics = "#{'${number.topic.name}'.split(',')}", groupId = Constants.GROUP_ID, containerFactory = "numberKafkaListenerContainerFactory")
    public void consume(Order order) {
       logger.info(String.format("Order consumed (from Number Producer)-> %s", order));
   }
}
