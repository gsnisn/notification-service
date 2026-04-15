package com.notificationservice.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class ExpenseCreatedDltConsumer {

    @KafkaListener(
            topics = "expense-created-dlt",
            groupId = "${spring.kafka.consumer.group-id}-dlt",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consumeDlt(String payload, @Headers Map<String, Object> headers) {
        log.error("Received expense-created DLT payload={}", payload);
        log.error("Headers={}", headers);

        // later: persist to DB / alert / manual review / reprocess logic
    }
}