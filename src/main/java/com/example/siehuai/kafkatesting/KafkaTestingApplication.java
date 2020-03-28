package com.example.siehuai.kafkatesting;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class KafkaTestingApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTestingApplication.class, args);
    }

    @Bean
    DataSource dataSource() {
        return DataSourceBuilder.create()
                .url("jdbc:mysql://localhost:3357/kafka_tutorial")
                .driverClassName("com.mysql.jdbc.Driver")
                .username("root")
                .password("pass")
                .build();
    }

    @Bean
    ProducerFactory producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", Collections.singletonList("localhost:9092"));

        // Important configuration to enable transaction in kafka
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "siehuai");
        return new DefaultKafkaProducerFactory<>(config, StringSerializer::new, StringSerializer::new);
    }

    @Bean
    ConsumerFactory consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", Collections.singletonList("localhost:9092"));
        config.put("group.id", "group_id");
        config.put("auto.offset.reset", "earliest");
        return new DefaultKafkaConsumerFactory<>(config, StringDeserializer::new, StringDeserializer::new);
    }

    @Bean
    @Qualifier("transactionManager")
    public ChainedKafkaTransactionManager<Object, Object> chainedTm(ProducerFactory<String, String> producerFactory) {
        return new ChainedKafkaTransactionManager<>(new KafkaTransactionManager<>(producerFactory),
                new DataSourceTransactionManager(dataSource()));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            ChainedKafkaTransactionManager<Object, Object> chainedTM) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.getContainerProperties().setTransactionManager(chainedTM);
        return factory;
    }
}
