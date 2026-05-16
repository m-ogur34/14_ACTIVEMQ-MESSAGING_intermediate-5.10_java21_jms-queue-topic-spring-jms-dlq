package com.activemq.consumer;

import com.activemq.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import jakarta.jms.Message;
import jakarta.jms.TextMessage;

/**
 * Message Consumers:
 *   - processOrder         → Temel Queue consumer, transactional
 *   - processNotification  → Property selector ile filtreleme
 *   - handleInventoryTopic → Topic subscriber (durable)
 *   - processWithDlq       → DLQ'ya yönlendirme mantığı
 *   - replyHandler         → Request-Reply consumer
 */
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);
    private static final int MAX_RETRIES = 3;

    private final JmsTemplate jmsTemplate;

    @Value("${app.queues.dlq}")
    private String dlqDestination;

    public OrderConsumer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    // Temel Queue consumer — containerFactory beanine referans
    @JmsListener(destination = "${app.queues.order}", containerFactory = "queueListenerFactory")
    public void processOrder(OrderMessage order) {
        log.info("Processing order: {}", order.getOrderId());
        try {
            doProcessOrder(order);
            log.info("Order processed successfully: {}", order.getOrderId());
        } catch (Exception e) {
            log.error("Order processing failed: {} — {}", order.getOrderId(), e.getMessage());
            throw e; // sessionTransacted=true → nack → broker redelivery
        }
    }

    private void doProcessOrder(OrderMessage order) {
        // Simulated business logic
        if (order.getAmount() == null || order.getAmount().signum() <= 0) {
            throw new IllegalArgumentException("Invalid amount: " + order.getAmount());
        }
        order.setStatus("CONFIRMED");
        log.info("Order confirmed: {} — amount: {}", order.getOrderId(), order.getAmount());
    }

    // Property Selector — sadece "region=EU" mesajlarını al
    @JmsListener(
            destination = "${app.queues.notification}",
            containerFactory = "queueListenerFactory",
            selector = "region = 'EU' AND tier = 'PREMIUM'"
    )
    public void processEuPremiumNotification(OrderMessage order, Message rawMessage) throws Exception {
        String region = rawMessage.getStringProperty("region");
        String tier = rawMessage.getStringProperty("tier");
        log.info("EU PREMIUM notification received — region: {}, tier: {}, order: {}",
                region, tier, order.getOrderId());
    }

    // Topic subscriber (durable)
    @JmsListener(
            destination = "${app.topics.inventory}",
            containerFactory = "topicListenerFactory",
            subscription = "inventory-service-sub"
    )
    public void handleInventoryUpdate(java.util.Map<String, Object> event) {
        log.info("Inventory update received: product={}, stock={}",
                event.get("productId"), event.get("newStock"));
    }

    // Manuel DLQ yönetimi — max retry aşıldıktan sonra DLQ'ya
    @JmsListener(destination = "${app.queues.order}", containerFactory = "queueListenerFactory")
    public void processWithRetryAndDlq(OrderMessage order, Message rawMessage) throws Exception {
        int retryCount = rawMessage.getIntProperty("JMSXDeliveryCount");
        log.info("Processing with retry — orderId: {}, attempt: {}", order.getOrderId(), retryCount);

        if (retryCount > MAX_RETRIES) {
            log.error("Max retries exceeded — sending to DLQ: {}", order.getOrderId());
            jmsTemplate.convertAndSend(dlqDestination, order, msg -> {
                msg.setStringProperty("dlq_reason", "max_retries_exceeded");
                msg.setIntProperty("original_retry_count", retryCount);
                return msg;
            });
            return; // ack — DLQ'ya koy, tekrar deneme
        }

        processOrder(order);
    }

    // Request-Reply pattern — reply-to queue'ya yanıt gönder
    @JmsListener(destination = "${app.queues.order}", containerFactory = "queueListenerFactory")
    public void replyHandler(TextMessage request) throws Exception {
        String payload = request.getText();
        jakarta.jms.Destination replyTo = request.getJMSReplyTo();
        String correlationId = request.getJMSCorrelationID();

        if (replyTo != null) {
            String response = "PROCESSED: " + payload;
            jmsTemplate.send(replyTo, session -> {
                TextMessage reply = session.createTextMessage(response);
                reply.setJMSCorrelationID(correlationId);
                return reply;
            });
        }
    }
}
