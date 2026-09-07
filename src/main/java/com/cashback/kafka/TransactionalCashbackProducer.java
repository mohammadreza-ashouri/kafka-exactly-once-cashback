package com.cashback.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionalCashbackProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionalCashbackProducer.class);

    public static final String TOPIC_CONVERSIONS = "conversions";
    public static final String TOPIC_CONFIRMED   = "cashback-confirmed";
    public static final String TOPIC_DLQ         = "cashback-dlq";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionalCashbackProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendConversion(String transactionId, String payload) {
        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(TOPIC_CONVERSIONS, transactionId, payload);
            log.info("Sent conversion txId={}", transactionId);
            return true;
        });
    }

    public void sendConfirmed(String transactionId, String userEmail, double cashback) {
        String payload = userEmail + "|" + cashback + "|CONFIRMED";
        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(TOPIC_CONFIRMED, transactionId, payload);
            log.info("Sent confirmed cashback txId={} user={} amount={}", transactionId, userEmail, cashback);
            return true;
        });
    }
}
