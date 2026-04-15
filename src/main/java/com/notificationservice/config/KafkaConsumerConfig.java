package com.notificationservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.splito.event.ExpenseCreatedEvent;
import com.splito.event.GroupCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration // Marks this as Spring configuration class (beans will be created from here)
@EnableKafka   // Enables Kafka listener processing (@KafkaListener starts working)
public class KafkaConsumerConfig {

    // -------------------------------
    // OBJECT MAPPER (for JSON parsing)
    // -------------------------------
    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // 🔴 VERY IMPORTANT
        // Adds support for Java time types like Instant, LocalDateTime etc.
        // Without this → your earlier error:
        // "Instant not supported"
        mapper.registerModule(new JavaTimeModule());

        return mapper;
    }

    // -------------------------------
    // ERROR HANDLER (RETRY + DLT)
    // -------------------------------
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaTemplate) {

        // This sends failed messages to DLT topic
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,

                        // Mapping logic:
                        // original topic → topic-dlt
                        (ConsumerRecord<?, ?> record, Exception ex) ->
                                new TopicPartition(record.topic() + "-dlt", record.partition())
                );

        // Retry config:
        // 2 sec delay, 3 retries
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));

        return errorHandler;
    }

    // ============================================================
    // 🔹 EXPENSE EVENT CONSUMER FACTORY
    // ============================================================

    @Bean
    public ConsumerFactory<String, ExpenseCreatedEvent> expenseConsumerFactory(ObjectMapper kafkaObjectMapper) {

        // Base Kafka config
        Map<String, Object> props = baseJsonConsumerProps("notification-group");

        // JSON → ExpenseCreatedEvent mapping
        JsonDeserializer<ExpenseCreatedEvent> jsonDeserializer =
                new JsonDeserializer<>(ExpenseCreatedEvent.class, kafkaObjectMapper, false);

        // Security: only allow trusted packages
        jsonDeserializer.addTrustedPackages("com.splito.event");

        // We are NOT using type headers (simpler)
        jsonDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                props,

                // Key deserializer (Kafka message key → String)
                new StringDeserializer(),

                // Wrap JSON deserializer with error handler
                // VERY IMPORTANT: handles deserialization failures
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    // ============================================================
    // 🔹 GROUP EVENT CONSUMER FACTORY
    // ============================================================

    @Bean
    public ConsumerFactory<String, GroupCreatedEvent> groupConsumerFactory(ObjectMapper kafkaObjectMapper) {

        Map<String, Object> props = baseJsonConsumerProps("notification-group");

        JsonDeserializer<GroupCreatedEvent> jsonDeserializer =
                new JsonDeserializer<>(GroupCreatedEvent.class, kafkaObjectMapper, false);

        jsonDeserializer.addTrustedPackages("com.splito.event");
        jsonDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    // ============================================================
    // 🔹 DLT CONSUMER FACTORY (STRING ONLY)
    // ============================================================

    @Bean
    public ConsumerFactory<String, String> dltConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        // Kafka connection
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Separate group for DLT consumers
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-group-dlt");

        // Start from beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Simple deserializers (NO JSON)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ============================================================
    // 🔹 EXPENSE CONTAINER
    // ============================================================

    @Bean(name = "expenseKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, ExpenseCreatedEvent>
    expenseKafkaListenerContainerFactory(
            ConsumerFactory<String, ExpenseCreatedEvent> expenseConsumerFactory,
            DefaultErrorHandler kafkaErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, ExpenseCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Attach consumer
        factory.setConsumerFactory(expenseConsumerFactory);

        // Attach retry + DLT logic
        factory.setCommonErrorHandler(kafkaErrorHandler);

        return factory;
    }

    // ============================================================
    // 🔹 GROUP CONTAINER
    // ============================================================

    @Bean(name = "groupKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, GroupCreatedEvent>
    groupKafkaListenerContainerFactory(
            ConsumerFactory<String, GroupCreatedEvent> groupConsumerFactory,
            DefaultErrorHandler kafkaErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, GroupCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(groupConsumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);

        return factory;
    }

    // ============================================================
    // 🔹 DLT CONTAINER (STRING BASED)
    // ============================================================

    @Bean(name = "dltKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String>
    dltKafkaListenerContainerFactory(
            ConsumerFactory<String, String> dltConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(dltConsumerFactory);

        return factory;
    }

    // ============================================================
    // 🔹 TOPIC CREATION
    // ============================================================

    @Bean
    public NewTopic expenseCreatedTopic() {
        return TopicBuilder.name("expense-created")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic expenseCreatedDltTopic() {
        return TopicBuilder.name("expense-created-dlt")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic groupCreatedTopic() {
        return TopicBuilder.name("group-created")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic groupCreatedDltTopic() {
        return TopicBuilder.name("group-created-dlt")
                .partitions(1)
                .replicas(1)
                .build();
    }

    // ============================================================
    // 🔹 COMMON BASE CONFIG
    // ============================================================

    private Map<String, Object> baseJsonConsumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Start from earliest if no offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Use ErrorHandlingDeserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        return props;
    }
}