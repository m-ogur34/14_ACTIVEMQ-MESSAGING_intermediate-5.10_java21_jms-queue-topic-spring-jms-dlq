package com.activemq.producer;

import com.activemq.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import jakarta.jms.*;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Message Producers:
 *   - sendOrder          → Queue (point-to-point), persistent
 *   - sendWithPriority   → JMS Priority 0-9 (0=low, 9=high)
 *   - sendWithDelay      → Scheduled delivery (AMQ SchedulerSupport)
 *   - publishToTopic     → Topic (pub-sub, all subscribers receive)
 *   - sendWithProperties → Custom JMS properties (selector filtresi için)
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final JmsTemplate jmsTemplate;
    private final JmsTemplate topicJmsTemplate;

    @Value("${app.queues.order}")
    private String orderQueue;

    @Value("${app.queues.notification}")
    private String notificationQueue;

    @Value("${app.topics.inventory}")
    private String inventoryTopic;

    @Value("${app.topics.broadcast}")
    private String broadcastTopic;

    public OrderProducer(JmsTemplate jmsTemplate,
                         @Qualifier("topicJmsTemplate") JmsTemplate topicJmsTemplate) {
        this.jmsTemplate = jmsTemplate;
        this.topicJmsTemplate = topicJmsTemplate;
    }

    // Temel mesaj gönderimi — Queue
    public void sendOrder(OrderMessage order) {
        jmsTemplate.convertAndSend(orderQueue, order);
        log.info("Order sent to queue: {}", order.getOrderId());
    }

    // JMS Priority ile öncelik belirleme
    public void sendWithPriority(OrderMessage order, int priority) {
        jmsTemplate.convertAndSend(orderQueue, order, message -> {
            message.setJMSPriority(priority); // 0-9
            return message;
        });
        log.info("Order sent with priority {}: {}", priority, order.getOrderId());
    }

    // Delayed delivery — ActiveMQ SchedulerSupport
    public void sendWithDelay(OrderMessage order, long delayMs) {
        jmsTemplate.convertAndSend(orderQueue, order, message -> {
            message.setLongProperty("AMQ_SCHEDULED_DELAY", delayMs);
            return message;
        });
        log.info("Order scheduled with {}ms delay: {}", delayMs, order.getOrderId());
    }

    // Topic yayını — tüm subscriber'lar alır
    public void publishInventoryUpdate(String productId, int newStock) {
        Map<String, Object> event = Map.of(
                "productId", productId,
                "newStock", newStock,
                "timestamp", System.currentTimeMillis()
        );
        topicJmsTemplate.convertAndSend(inventoryTopic, event);
        log.info("Inventory update published: product={}, stock={}", productId, newStock);
    }

    // Custom JMS properties — consumer selector filtresi için
    public void sendWithProperties(OrderMessage order, String region, String tier) {
        jmsTemplate.convertAndSend(notificationQueue, order, message -> {
            message.setStringProperty("region", region);
            message.setStringProperty("tier", tier);
            message.setIntProperty("retryCount", order.getRetryCount());
            return message;
        });
    }

    // Request-Reply pattern (senkron)
    public String sendAndReceive(String payload) {
        Object reply = jmsTemplate.sendAndReceive(orderQueue, session -> {
            TextMessage msg = session.createTextMessage(payload);
            msg.setJMSReplyTo(session.createTemporaryQueue());
            return msg;
        });
        return reply != null ? reply.toString() : null;
    }

    // DLQ'ya elle mesaj gönder (test/admin amaçlı)
    public void sendToDlq(OrderMessage order, String reason) {
        jmsTemplate.convertAndSend("dead.letter.queue", order, message -> {
            message.setStringProperty("dlq_reason", reason);
            message.setStringProperty("original_queue", orderQueue);
            return message;
        });
        log.warn("Message sent to DLQ — reason: {}, orderId: {}", reason, order.getOrderId());
    }

    // Batch sending
    public void sendBatch(java.util.List<OrderMessage> orders) {
        orders.forEach(this::sendOrder);
        log.info("Batch sent: {} messages", orders.size());
    }
}
