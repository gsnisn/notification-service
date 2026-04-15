package com.notificationservice.kafka;

import com.splito.event.GroupCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class GroupCreatedConsumer {

    @KafkaListener(
            topics = "${app.kafka.topics.group-created}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "groupKafkaListenerContainerFactory"
    )
    public void consume(GroupCreatedEvent event) {
        log.info("Received group-created event: groupId={}, name={}",
                event.getGroupId(),
                event.getGroupName());

        log.info("Dummy notification sent for group {}", event.getGroupId());
    }
}