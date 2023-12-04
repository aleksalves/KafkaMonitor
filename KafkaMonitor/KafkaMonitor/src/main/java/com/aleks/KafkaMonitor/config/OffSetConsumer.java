package com.aleks.KafkaMonitor.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class OffSetConsumer {

    private AdminClient adminClient;
    private KafkaConsumer<String, String> kafkaConsumer;
    private String bootstrapServer;
    private String topic;
    private String consumerGroupId;

    public OffSetConsumer(String bootstrapServer, String topic, String consumerGroupId){
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        adminClient = getAdminClient(this.bootstrapServer);
        this.kafkaConsumer = getKafkaConsumer();
    }

    private AdminClient getAdminClient(String bootstrapServerConfig) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
        return AdminClient.create(config);
    }

    private Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
            throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, Long> groupOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();
            if(key.topic().equals(topic)){
                groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
            }
        }
        return groupOffset;
    }

    private KafkaConsumer<String, String> getKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {
        List<TopicPartition> topicPartitions = new LinkedList<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
            TopicPartition key = entry.getKey();
            if(key.topic().equals(topic)){
                topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
            }
        }
        return kafkaConsumer.endOffsets(topicPartitions);
    }

    private Map<TopicPartition, Long> computeLags(
            Map<TopicPartition, Long> consumerGrpOffsets,
            Map<TopicPartition, Long> producerOffsets) {
        Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(producerOffset - consumerOffset);
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }
    public void analyzeLag() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(consumerGroupId);
        Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
        Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
        Long lagTotal = 0L;
        for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
            lagTotal += lagEntry.getValue();
        }
        System.out.println("Tópico: ");
        System.out.println(topic);
        System.out.println("Lag: ");
        System.out.println(lagTotal);
    }
}
