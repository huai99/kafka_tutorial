package com.example.siehuai.kafkatesting.kafka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
public class Consumer implements ConsumerSeekAware {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "person", groupId = "group_id")
    public void consumePersonMsg(String message) throws IOException {
        logger.info(String.format("#### -> Consumed message -> %s", message));
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // This is to subscribe from the very beginning
        assignments.keySet().stream()
                .filter(partition -> "users".equals(partition.topic()))
                .forEach(partition -> callback.seekToBeginning("users", partition.partition()));
    }
}
