package com.cashback.controller;

import com.cashback.kafka.TransactionalCashbackProducer;
import com.cashback.service.CashbackService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
public class PostbackController {

    private static final Logger log = LoggerFactory.getLogger(PostbackController.class);

    private final TransactionalCashbackProducer producer;
    private final CashbackService cashbackService;

    public PostbackController(
        TransactionalCashbackProducer producer,
        CashbackService cashbackService
    ) {
        this.producer        = producer;
        this.cashbackService = cashbackService;
    }

    @PostMapping("/postback")
    public ResponseEntity<Map<String, String>> receivePostback(
        @RequestParam String transactionId,
        @RequestParam String userEmail,
        @RequestParam String partnerId,
        @RequestParam double amount,
        @RequestParam double rate
    ) {
        String payload = userEmail + "|" + partnerId + "|" + amount + "|" + rate;
        producer.sendConversion(transactionId, payload);
        log.info("Postback queued txId={} user={} partner={}", transactionId, userEmail, partnerId);

        return ResponseEntity.accepted().body(Map.of(
            "status", "QUEUED",
            "transactionId", transactionId
        ));
    }

    @PostMapping("/simulate")
    public ResponseEntity<Map<String, String>> simulate(
        @RequestParam(defaultValue = "test@shoop.de") String userEmail,
        @RequestParam(defaultValue = "eBay") String partnerId,
        @RequestParam(defaultValue = "100.0") double amount,
        @RequestParam(defaultValue = "0.03") double rate
    ) {
        String txId    = "sim-" + UUID.randomUUID();
        String payload = userEmail + "|" + partnerId + "|" + amount + "|" + rate;
        producer.sendConversion(txId, payload);

        return ResponseEntity.accepted().body(Map.of(
            "status", "QUEUED",
            "transactionId", txId,
            "expectedCashback", String.format("%.2f EUR", amount * rate)
        ));
    }

    @GetMapping("/balance/{email}")
    public ResponseEntity<Map<String, Object>> getBalance(@PathVariable String email) {
        double pending = cashbackService.getPendingBalance(email);
        return ResponseEntity.ok(Map.of(
            "userEmail", email,
            "pendingCashback", pending,
            "currency", "EUR"
        ));
    }

    @GetMapping("/health/stale-pending")
    public ResponseEntity<Map<String, Object>> stalePending() {
        long count = cashbackService.getStalePendingCount();
        return ResponseEntity.ok(Map.of(
            "stalePendingTransactions", count,
            "alert", count > 100 ? "WARNING" : "OK"
        ));
    }
}
