package com.activemq.controller;

import com.activemq.model.OrderMessage;
import com.activemq.producer.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/messaging")
public class MessagingController {

    private final OrderProducer producer;

    public MessagingController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/order")
    public ResponseEntity<Map<String, Object>> sendOrder(
            @RequestParam String email,
            @RequestParam BigDecimal amount) {
        OrderMessage order = new OrderMessage(email, amount);
        producer.sendOrder(order);
        return ResponseEntity.ok(Map.of("orderId", order.getOrderId(), "status", "queued"));
    }

    @PostMapping("/order/priority")
    public ResponseEntity<Map<String, Object>> sendWithPriority(
            @RequestParam String email,
            @RequestParam BigDecimal amount,
            @RequestParam(defaultValue = "5") int priority) {
        OrderMessage order = new OrderMessage(email, amount);
        producer.sendWithPriority(order, priority);
        return ResponseEntity.ok(Map.of("orderId", order.getOrderId(), "priority", priority));
    }

    @PostMapping("/order/delay")
    public ResponseEntity<Map<String, Object>> sendWithDelay(
            @RequestParam String email,
            @RequestParam BigDecimal amount,
            @RequestParam(defaultValue = "5000") long delayMs) {
        OrderMessage order = new OrderMessage(email, amount);
        producer.sendWithDelay(order, delayMs);
        return ResponseEntity.ok(Map.of("orderId", order.getOrderId(), "delayMs", delayMs));
    }

    @PostMapping("/inventory")
    public ResponseEntity<Void> publishInventory(
            @RequestParam String productId,
            @RequestParam int stock) {
        producer.publishInventoryUpdate(productId, stock);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatch(
            @RequestParam int count,
            @RequestParam(defaultValue = "batch@test.com") String email) {
        List<OrderMessage> orders = IntStream.range(0, count)
                .mapToObj(i -> new OrderMessage(email, BigDecimal.valueOf(100 + i)))
                .toList();
        producer.sendBatch(orders);
        return ResponseEntity.ok(Map.of("sent", count));
    }
}
