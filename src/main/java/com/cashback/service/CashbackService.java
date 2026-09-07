package com.cashback.service;

import com.cashback.entity.CashbackTransaction;
import com.cashback.kafka.TransactionalCashbackProducer;
import com.cashback.repository.CashbackTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CashbackService {

    private static final Logger log = LoggerFactory.getLogger(CashbackService.class);

    private final CashbackTransactionRepository repository;
    private final TransactionalCashbackProducer producer;

    public CashbackService(
        CashbackTransactionRepository repository,
        TransactionalCashbackProducer producer
    ) {
        this.repository = repository;
        this.producer   = producer;
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void processCashback(
        String transactionId,
        String userEmail,
        String partnerId,
        double amount,
        double rate
    ) {
        if (repository.existsByTransactionId(transactionId)) {
            log.info("Idempotency hit txId={} already processed", transactionId);
            return;
        }

        double cashbackAmount = Math.round(amount * rate * 100.0) / 100.0;

        CashbackTransaction tx = new CashbackTransaction();
        tx.setTransactionId(transactionId);
        tx.setUserEmail(userEmail);
        tx.setPartnerId(partnerId);
        tx.setPurchaseAmount(amount);
        tx.setCashbackAmount(cashbackAmount);
        tx.setPartnerRateSnapshot(rate);
        tx.setStatus("PENDING");
        repository.save(tx);

        log.info("Cashback credited txId={} user={} amount={}EUR status=PENDING",
            transactionId, userEmail, cashbackAmount);

        producer.sendConfirmed(transactionId, userEmail, cashbackAmount);
    }

    public double getPendingBalance(String userEmail) {
        return repository.findByUserEmailAndStatus(userEmail, "PENDING")
            .stream()
            .mapToDouble(CashbackTransaction::getCashbackAmount)
            .sum();
    }

    public long getStalePendingCount() {
        return repository.countStalePendingTransactions();
    }
}
