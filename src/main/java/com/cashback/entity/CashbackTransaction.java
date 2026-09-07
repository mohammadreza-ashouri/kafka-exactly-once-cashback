package com.cashback.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(
    name = "cashback_transactions",
    indexes = @Index(name = "idx_transaction_id", columnList = "transactionId", unique = true)
)
public class CashbackTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String transactionId;

    @Column(nullable = false)
    private String userEmail;

    @Column(nullable = false)
    private String partnerId;

    @Column(nullable = false)
    private double purchaseAmount;

    @Column(nullable = false)
    private double cashbackAmount;

    @Column(nullable = false)
    private double partnerRateSnapshot;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private LocalDateTime processedAt = LocalDateTime.now();

    @Version
    private int version;

    public Long getId() { return id; }
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getUserEmail() { return userEmail; }
    public void setUserEmail(String userEmail) { this.userEmail = userEmail; }
    public String getPartnerId() { return partnerId; }
    public void setPartnerId(String partnerId) { this.partnerId = partnerId; }
    public double getPurchaseAmount() { return purchaseAmount; }
    public void setPurchaseAmount(double purchaseAmount) { this.purchaseAmount = purchaseAmount; }
    public double getCashbackAmount() { return cashbackAmount; }
    public void setCashbackAmount(double cashbackAmount) { this.cashbackAmount = cashbackAmount; }
    public double getPartnerRateSnapshot() { return partnerRateSnapshot; }
    public void setPartnerRateSnapshot(double rate) { this.partnerRateSnapshot = rate; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public LocalDateTime getProcessedAt() { return processedAt; }
    public int getVersion() { return version; }
}
