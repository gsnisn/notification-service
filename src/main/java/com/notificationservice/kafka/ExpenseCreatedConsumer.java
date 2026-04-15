package com.notificationservice.kafka;

import com.splito.event.ExpenseCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ExpenseCreatedConsumer {

    @KafkaListener(
            topics = "${app.kafka.topics.expense-created}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "expenseKafkaListenerContainerFactory"
    )
    public void consume(ExpenseCreatedEvent event) {
        log.info("Received expense-created event: expenseId={}, groupId={}, paidByUserId={}, amount={}, participants={}",
                event.getExpenseId(),
                event.getGroupId(),
                event.getPaidByUserId(),
                event.getAmount(),
                event.getParticipantUserIds());

        //simulate failure
        if (event.getAmount() != null && event.getAmount().intValue() == 600) {
            throw new RuntimeException("Simulated notification failure");
        }

        log.info("Dummy notification sent for expense {}", event.getExpenseId());
    }
}