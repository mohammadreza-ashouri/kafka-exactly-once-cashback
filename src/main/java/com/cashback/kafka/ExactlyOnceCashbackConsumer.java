package com.cashback.kafka;

import com.cashback.service.CashbackService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class ExactlyOnceCashbackConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceCashbackConsumer.class);

    private final CashbackService cashbackService;

    public ExactlyOnceCashbackConsumer(CashbackService cashbackService) {
        this.cashbackService = cashbackService;
    }

    @KafkaListener(
        topics = TransactionalCashbackProducer.TOPIC_CONVERSIONS,
        groupId = "cashback-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onConversion(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String transactionId = record.key();
        String payload       = record.value();

        log.info("Received conversion txId={} offset={} partition={}",
            transactionId, record.offset(), record.partition());

        try {
            String[] parts   = payload.split("\\|");
            String userEmail = parts[0];
            String partnerId = parts[1];
            double amount    = Double.parseDouble(parts[2]);
            double rate      = Double.parseDouble(parts[3]);

            cashbackService.processCashback(transactionId, userEmail, partnerId, amount, rate);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process txId={} reason={}", transactionId, e.getMessage());
            throw e;
        }
    }

    @KafkaListener(
        topics = TransactionalCashbackProducer.TOPIC_CONVERSIONS + "-dlt",
        groupId = "cashback-dlq-service"
    )
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.error("DEAD LETTER txId={} payload={}", record.key(), record.value());
        ack.acknowledge();
    }
}
