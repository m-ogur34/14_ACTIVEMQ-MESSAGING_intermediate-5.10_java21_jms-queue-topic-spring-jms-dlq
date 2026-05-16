package com.activemq.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class OrderMessage {

    private String orderId;
    private String customerEmail;
    private BigDecimal amount;
    private String status;
    private LocalDateTime timestamp;
    private int retryCount;

    public OrderMessage() {}

    public OrderMessage(String customerEmail, BigDecimal amount) {
        this.orderId = UUID.randomUUID().toString();
        this.customerEmail = customerEmail;
        this.amount = amount;
        this.status = "PENDING";
        this.timestamp = LocalDateTime.now();
        this.retryCount = 0;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getCustomerEmail() { return customerEmail; }
    public void setCustomerEmail(String customerEmail) { this.customerEmail = customerEmail; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    @Override
    public String toString() {
        return "OrderMessage{orderId='%s', email='%s', amount=%s, status='%s'}"
                .formatted(orderId, customerEmail, amount, status);
    }
}
