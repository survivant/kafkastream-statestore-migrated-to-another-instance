package com.example.kafkaeventalarm.admin;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaAdminClient {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;


    public ListTopicsResult getTopics(){
        AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress));

        return admin.listTopics();
    }

    public DescribeTopicsResult getDescribeTopic(String topic){
        AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress));

        return admin.describeTopics(Arrays.asList(topic));
    }

    public ListConsumerGroupsResult getConsumeGroups(){
        AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress));

        return admin.listConsumerGroups();
    }

}
