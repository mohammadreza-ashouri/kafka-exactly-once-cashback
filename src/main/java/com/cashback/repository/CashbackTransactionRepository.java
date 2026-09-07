package com.cashback.repository;

import com.cashback.entity.CashbackTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface CashbackTransactionRepository extends JpaRepository<CashbackTransaction, Long> {

    boolean existsByTransactionId(String transactionId);

    List<CashbackTransaction> findByUserEmailAndStatus(String userEmail, String status);

    @Query("SELECT COUNT(t) FROM CashbackTransaction t WHERE t.status = 'PENDING' " +
           "AND t.processedAt < CURRENT_TIMESTAMP - 1 DAY")
    long countStalePendingTransactions();
}
