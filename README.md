# 14 — ActiveMQ Messaging

**Difficulty:** Intermediate (5/10) · **Java 21** · **Spring Boot 3.2.5**

JMS (Java Message Service) ile mesaj kuyruğu, topic yayınları, DLQ ve ileri düzey mesajlaşma desenleri.

---

## Mesajlaşma Kavramları

### Queue vs Topic
```
Queue (Point-to-Point)        Topic (Publish-Subscribe)
─────────────────────────     ─────────────────────────────
Producer → [Queue] →           Publisher → [Topic] → Sub A
           Consumer A                              → Sub B
           (ya da B, ikisi     → Sub C (durable)
            aynı anda almaz)
Tek alıcı, yük dağılımı      Tüm subscriber'lar alır
```

### JMS Delivery Modes
| Mode | Açıklama |
|------|----------|
| `PERSISTENT` | Broker restart'ında kaybolmaz (default) |
| `NON_PERSISTENT` | Hız için, broker kapanırsa kaybolabilir |

---

## Spring Boot JMS Yapılandırması

### Message Converter (Jackson JSON)
```java
@Bean
public MappingJackson2MessageConverter jacksonJmsMessageConverter() {
    MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
    converter.setTargetType(MessageType.TEXT);     // TextMessage (JSON string)
    converter.setTypeIdPropertyName("_type");      // POJO tipi header'da
    return converter;
}
```

### Listener Factory — Queue vs Topic
```java
// Queue: pub-sub=false, concurrent, transactional
factory.setPubSubDomain(false);
factory.setConcurrency("3-10");       // min 3, max 10 listener thread
factory.setSessionTransacted(true);   // exception → nack → broker redelivery

// Topic: pub-sub=true, durable subscription
factory.setPubSubDomain(true);
factory.setSubscriptionDurable(true);
factory.setClientId("my-service");    // durable sub için zorunlu
```

---

## Producer Desenleri

### Temel Gönderim
```java
jmsTemplate.convertAndSend(queueName, orderMessage);
```

### Message Post-Processor (custom headers)
```java
jmsTemplate.convertAndSend(queue, order, message -> {
    message.setJMSPriority(9);                          // yüksek öncelik
    message.setLongProperty("AMQ_SCHEDULED_DELAY", 5000L); // 5sn gecikme
    message.setStringProperty("region", "EU");           // selector filtresi
    return message;
});
```

### JMS Priority (0-9)
```
0-4 → Normal öncelik
5-9 → Yüksek öncelik (broker kuyruğunu yeniden sıralar)
```

### Topic Publish
```java
topicJmsTemplate.setPubSubDomain(true);
topicJmsTemplate.convertAndSend(topicName, event);
// Tüm aktif + durable subscriber'lar alır
```

---

## Consumer Desenleri

### Temel Listener
```java
@JmsListener(
    destination = "${app.queues.order}",
    containerFactory = "queueListenerFactory"
)
public void processOrder(OrderMessage order) {
    // exception fırlatılırsa → sessionTransacted=true → nack → redelivery
}
```

### Property Selector (Filtreli Tüketim)
```java
@JmsListener(
    destination = "${app.queues.notification}",
    selector = "region = 'EU' AND tier = 'PREMIUM'"
)
public void processEuPremium(OrderMessage order) { ... }
```

### Durable Topic Subscription
```java
@JmsListener(
    destination = "${app.topics.inventory}",
    containerFactory = "topicListenerFactory",
    subscription = "inventory-service-sub"  // durable sub adı
)
public void handleInventory(Map<String, Object> event) { ... }
// Subscriber offline iken gelen mesajlar tutulur
```

---

## Dead Letter Queue (DLQ)

```
Normal akış:
Producer → [Order Queue] → Consumer → başarılı

Hata durumu (sessionTransacted=true):
Producer → [Order Queue] → Consumer (exception)
                         ↓ (nack → redelivery)
                    Max retry aşıldı
                         ↓
                   [Dead Letter Queue]
```

```java
// Manuel DLQ yönetimi
int retryCount = rawMessage.getIntProperty("JMSXDeliveryCount");
if (retryCount > MAX_RETRIES) {
    jmsTemplate.convertAndSend("dead.letter.queue", order, msg -> {
        msg.setStringProperty("dlq_reason", "max_retries_exceeded");
        return msg;
    });
    return; // ack — tekrar deneme yok
}
```

ActiveMQ Default DLQ: `ActiveMQ.DLQ`
Per-destination DLQ: `ActiveMQ.DLQ.order.queue`

---

## Request-Reply Pattern

```
Requester                    Responder
─────────                    ─────────
JMSReplyTo = temp queue  →  processRequest()
JMSCorrelationID = X     →  reply to JMSReplyTo
                         ←  JMSCorrelationID = X
```

```java
// Gönderici — reply-to header ekle
jmsTemplate.sendAndReceive(queue, session -> {
    TextMessage msg = session.createTextMessage(payload);
    msg.setJMSReplyTo(session.createTemporaryQueue());
    return msg;
});
```

---

## REST Endpoints

| Method | URL | Açıklama |
|--------|-----|----------|
| POST | `/api/messaging/order?email=&amount=` | Queue'ya mesaj gönder |
| POST | `/api/messaging/order/priority?priority=9` | Öncelikli mesaj |
| POST | `/api/messaging/order/delay?delayMs=5000` | Gecikmeli mesaj |
| POST | `/api/messaging/inventory?productId=&stock=` | Topic yayını |
| POST | `/api/messaging/batch?count=100` | Toplu mesaj |

---

## Mülakat Soruları

**Q: Queue ile Topic farkı?**
A: Queue — tek tüketici alır, load balance edilir. Topic — tüm subscriber'lar alır, pub-sub model.

**Q: `sessionTransacted=true` ne anlama gelir?**
A: Consumer exception fırlatırsa mesaj nack'd → broker yeniden iletir. `false`'ta mesaj kaybolabilir.

**Q: Durable subscription nedir?**
A: Topic subscriber offline iken gelen mesajlar tutulur. `clientId` + `subscription` adı zorunlu.

**Q: DLQ ne zaman kullanılır?**
A: Max redelivery aşıldıktan sonra işlenemeyen mesajlar buraya düşer. Operasyonel inceleme + replay için.

**Q: JMS Priority nasıl çalışır?**
A: Broker yüksek öncelikli (5-9) mesajları öne alır. Garantili değil, en iyi çaba prensibi.

**Q: `JMSXDeliveryCount` nedir?**
A: JMS property — mesajın kaç kez teslim edildiğini tutar. Retry sayacı olarak kullanılır.

---

## Çalıştırma

```bash
# ActiveMQ başlat
docker run -d -p 61616:61616 -p 8161:8161 apache/activemq-classic

# Admin UI
open http://localhost:8161  # admin/admin

# Uygulama başlat
mvn spring-boot:run

# Test
curl -X POST "http://localhost:8080/api/messaging/order?email=test@test.com&amount=100"
```
