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

**Q: Queue ile Topic farkı nedir? Ne zaman hangisi kullanılır?**
A: Queue (Point-to-Point): Mesaj tek bir consumer'a gider — birden fazla consumer varsa load balance edilir (yarışır). Sipariş işleme, e-posta gönderimi gibi "bir kez yapılacak işler" için. Topic (Publish-Subscribe): Mesaj tüm subscriber'lara gider — publisher kim dinlediğini bilmez. Event broadcast, audit log, cache invalidation gibi "herkese bildir" senaryoları için. Kafka vs ActiveMQ: Kafka yüksek throughput/replay için, ActiveMQ düşük latency/JMS compliance/transactional messaging için tercih edilir.

**Q: `sessionTransacted=true` ne anlama gelir? At-least-once delivery nasıl sağlanır?**
A: `sessionTransacted=true`: Consumer bir mesajı aldıktan sonra exception fırlatırsa, JMS session'ı rollback olur, mesaj broker'a iade edilir ve yeniden iletilir (redelivery). `false` (auto-acknowledge): Mesaj alındıktan hemen sonra acknowledge edilir — consumer başarısız olsa bile mesaj kaybolur (at-most-once). At-least-once delivery için: `sessionTransacted=true` + `@JmsListener` metodu exception fırlatsın → broker yeniden iletir. Idempotency: Aynı mesaj birden fazla gelebilir — iş mantığı idempotent olmalı (aynı sipariş ID'si ile iki kez sipariş oluşturma).

**Q: Durable subscription nedir? Non-durable'dan farkı?**
A: Durable subscription: Topic subscriber offline iken gelen mesajlar broker tarafından tutulur, subscriber tekrar bağlandığında alır. Gereksinim: `clientId` (bağlantı bazlı unique ID) + `subscriptionName` (abonelik adı) belirlenmeli. Non-durable: Subscriber offline iken gelen mesajlar kaybolur — "fire and forget" yayınlar için. Kullanım: Kritik domain eventleri (sipariş oluşturuldu) → durable; anlık dashboard güncellemeleri → non-durable. ActiveMQ durable topic'leri disk'e yazar, broker restart'ından kurtarır.

**Q: DLQ (Dead Letter Queue) neden gereklidir? Operasyonel önemi nedir?**
A: DLQ: Max redelivery sayısına ulaşan (default 6) ve işlenemeyen mesajların düştüğü kuyruk. Neden önemli: Poison message (zararlı mesaj — her denemede exception fırlatır) normal kuyruğu tıkar, diğer mesajlar işlenemez. DLQ sayesinde sorunlu mesaj ayrılır, sağlıklı mesajlar işlenmeye devam eder. Operasyonel pratik: (1) Alert: DLQ dolunca alarm tetikle. (2) İnceleme: Mesaj içeriği ve exception stack trace'i logla/yaz. (3) Replay: Hata düzeltildikten sonra DLQ'dan orijinal kuyruğa tekrar gönder. Spring'de özel DLQ listener yazılabilir.

**Q: ActiveMQ vs Kafka temel farkları nelerdir?**
A: ActiveMQ (JMS broker): Push model — broker mesajı consumer'a iter. Mesaj consumer alınca silinir (queue) veya belirtilen süre tutulur. Priority, durable sub, transactional messaging. Düşük-orta throughput (~10K msg/s), eski enterprise sistemlerle uyum. Kafka (distributed log): Pull model — consumer kendi hızında çeker. Mesajlar retention period boyunca saklanır (replay mümkün). Consumer group offset ile "nerede kaldım" izlenir. Yüksek throughput (milyonlarca msg/s), event sourcing, audit trail. Seçim: Kurumsal JMS uyumu, düşük latency, transactional → ActiveMQ. Big data pipeline, event sourcing, replay, yüksek throughput → Kafka.

**Q: JMS Priority ile message ordering nasıl çalışır?**
A: JMS Priority: 0-9 arası (0 en düşük, 9 en yüksek, default 4). Broker yüksek öncelikli mesajları consumer'a önce iletmeye çalışır — garanti değil, best-effort. Ordering: Queue içinde FIFO garantisi vardır (aynı priority). Birden fazla consumer varsa ordering bozulur (paralel tüketim). Ordering gerekiyorsa tek consumer veya message key ile routing. `JMSXDeliveryCount`: Standart JMS property, mesajın kaçıncı teslimat denemesi olduğunu tutar — DLQ'ya düşme kararı için kullanılır (`if (count >= 3) putToDLQ()`).

**Q: Request-Reply pattern JMS'de nasıl uygulanır?**
A: Producer `JMSReplyTo` header'ına geçici kuyruk (TemporaryQueue) adresi yazar ve `JMSCorrelationID` ile mesajı gönderir. Consumer işleyip sonucu `JMSReplyTo`'daki kuyruğa yazar, aynı `JMSCorrelationID`'yi ekler. Producer geçici kuyruktan sonucu bekler. Spring `JmsTemplate.sendAndReceive()` bunu otomatik yapar. Dezavantaj: Consumer meşgulse producer bloklanır — asenkron sistemlerde zaman aşımı kritik. Kullanım: Anlık hesaplama sonucu beklemek zorunda olan servisler arası RPC.

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
