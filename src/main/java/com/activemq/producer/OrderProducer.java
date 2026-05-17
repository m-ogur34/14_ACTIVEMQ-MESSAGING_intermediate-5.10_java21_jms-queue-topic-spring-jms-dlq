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
 * JMS MESAJ ÜRETİCİSİ — ActiveMQ Producer Desenleri
 * ====================================================
 *
 * Mesaj kuyruğu neden HTTP yerine kullanılır?
 *   HTTP (senkron): A servisi B servisine istek gönderir, B düşükse A da bloke olur.
 *   Mesaj kuyruğu (asenkron): A kuyruğa bırakır, B hazır olunca alır.
 *   Avantajlar:
 *     - Servisler birbirinden bağımsız — B düşük olsa bile A çalışmaya devam eder.
 *     - Yük dengeleme: Yüksek trafik kuyruğa birikir, consumer kendi hızında işler.
 *     - Tekrar deneme: İşlenemeyen mesaj otomatik yeniden iletilir.
 *
 * Queue vs Topic seçim kriteri:
 *   Queue → "Her sipariş tam olarak bir kez işlenmeli" → Point-to-point
 *   Topic → "Stok değişti, ilgili tüm servisler bilmeli" → Publish-Subscribe
 *
 * İki JmsTemplate neden?
 *   jmsTemplate: Queue için (pubSubDomain=false)
 *   topicJmsTemplate: Topic için (pubSubDomain=true)
 *   Aynı template her ikisi için kullanılabilir ama konfigürasyon ayrı tutmak daha net.
 *
 * Bu sınıftaki desenler:
 *   sendOrder          → Temel queue gönderimi (persistent, guaranteed delivery)
 *   sendWithPriority   → Yüksek öncelikli mesajlar önce işlenir
 *   sendWithDelay      → Zamanlanmış mesaj (örn: 5 dakika sonra işle)
 *   publishInventoryUpdate → Topic yayını (tüm subscriber'lar alır)
 *   sendWithProperties → Broker-tarafı filtreleme için property ekle
 *   sendAndReceive     → Asenkron transport üzerinde senkron yanıt bekleme
 *   sendToDlq          → Ölü mesajları yönetim için DLQ'ya elle koy
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final JmsTemplate jmsTemplate;
    private final JmsTemplate topicJmsTemplate; // pubSubDomain=true olan template

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

    /**
     * Temel queue mesaj gönderimi — Guaranteed Delivery.
     *
     * convertAndSend ne yapar?
     *   OrderMessage nesnesini → JSON string'e dönüştürür (MessageConverter).
     *   JSON → TextMessage olarak kuyruğa koyar.
     *   Consumer tarafında: TextMessage → JSON → OrderMessage (ters dönüşüm).
     *
     * Persistent delivery (varsayılan):
     *   Mesaj broker'ın diskine yazılır — broker yeniden başlasa bile kaybolmaz.
     *   Non-persistent: Sadece bellekte → broker yeniden başlarsa mesaj kaybolur.
     *   Bankacılık/sipariş: Her zaman persistent kullan — veri kaybı kabul edilemez.
     */
    public void sendOrder(OrderMessage order) {
        jmsTemplate.convertAndSend(orderQueue, order);
        log.info("Order sent to queue: {}", order.getOrderId());
    }

    /**
     * JMS Priority ile mesaj önceliklendirme.
     *
     * JMS Priority 0-9 nedir?
     *   0: En düşük öncelik
     *   4: Varsayılan öncelik
     *   9: En yüksek öncelik
     *   Broker: Yüksek öncelikli mesajları düşük öncelikli mesajlardan önce iletir.
     *
     * Ne zaman kullanılır?
     *   Acil sipariş iptali: priority=9 → Diğer normal siparişlerden önce işlenir.
     *   Toplu raporlama: priority=1 → Normal siparişler için yer açılır.
     *   Dikkat: Priority garanti değil — sadece "tercihen önce" anlamı taşır.
     *
     * MessagePostProcessor lambda neden?
     *   convertAndSend sadece nesne alır — JMS property'leri set etmek için lambda gerekir.
     *   Lambda: message oluşturulduktan sonra ek ayarlar yapmak için hook noktası.
     */
    public void sendWithPriority(OrderMessage order, int priority) {
        jmsTemplate.convertAndSend(orderQueue, order, message -> {
            message.setJMSPriority(priority); // 0-9 arası değer
            return message;
        });
        log.info("Order sent with priority {}: {}", priority, order.getOrderId());
    }

    /**
     * Zamanlanmış mesaj gönderimi — ActiveMQ SchedulerSupport.
     *
     * Neden delayed delivery?
     *   Ödeme onayı sonrası sipariş işleme: 5 dakika bekle, ödeme iptal edilmezse işle.
     *   Retry with backoff: İlk deneme başarısız → 1 dakika sonra tekrar dene.
     *   Hatırlatma bildirimi: 24 saat sonra "siparişiniz onaylandı" emaili gönder.
     *
     * AMQ_SCHEDULED_DELAY: ActiveMQ'ya özgü JMS property.
     *   Milisaniye cinsinden gecikme: 60_000 = 1 dakika.
     *   ActiveMQ bu property'i görünce mesajı hemen iletmez, süre dolunca iletir.
     *   Dikkat: Sadece ActiveMQ'ya özgü — RabbitMQ veya Kafka'da çalışmaz.
     *   Alternatif: application seviyesinde ScheduledExecutorService ile geciktirme.
     */
    public void sendWithDelay(OrderMessage order, long delayMs) {
        jmsTemplate.convertAndSend(orderQueue, order, message -> {
            message.setLongProperty("AMQ_SCHEDULED_DELAY", delayMs);
            return message;
        });
        log.info("Order scheduled with {}ms delay: {}", delayMs, order.getOrderId());
    }

    /**
     * Topic yayını — Publish-Subscribe (Tüm subscriber'lar alır).
     *
     * Queue ile fark:
     *   Queue: 3 consumer varsa → mesaj yalnızca 1'ine gider (yük dağılımı).
     *   Topic: 3 subscriber varsa → mesaj 3'üne de gider (broadcast).
     *
     * Stok güncellemesi için neden Topic?
     *   inventory-service güncelledi → sipariş servisi, bildirim servisi, dashboard hepsi bilmeli.
     *   Queue kullansaydık: 3 ayrı kuyruk, 3 ayrı mesaj gönderimi gerekir (üretici tüm servisleri bilmeli).
     *   Topic: Üretici bir kez yayınlar, kim dinliyorsa alır — üretici subscriber'ları bilmez.
     *   Servis ekleme: Yeni bir servis topic'i dinlemeye başlar → üretici kodu değişmez.
     *
     * topicJmsTemplate: pubSubDomain=true olan template kullanılmalı.
     *   jmsTemplate (queue) ile topic'e gönderilirse mesaj yanlış iletilir.
     */
    public void publishInventoryUpdate(String productId, int newStock) {
        Map<String, Object> event = Map.of(
                "productId", productId,
                "newStock", newStock,
                "timestamp", System.currentTimeMillis()
        );
        topicJmsTemplate.convertAndSend(inventoryTopic, event);
        log.info("Inventory update published: product={}, stock={}", productId, newStock);
    }

    /**
     * Custom JMS Properties — Broker-Tarafı Filtreleme İçin.
     *
     * Neden property eklemek gerekir?
     *   Consumer-tarafı filtre: Her mesaj consumer'a gelir, consumer ayıklar → ağ israfı.
     *   Broker-tarafı selector: Consumer yalnızca ilgilendiği mesajları alır.
     *   Örnek: EU bölgesi PREMIUM tier consumer → selector = "region='EU' AND tier='PREMIUM'"
     *   Broker bu selector'ı karşılamayan mesajları o consumer'a göndermez.
     *
     * Property tipleri:
     *   setStringProperty("region", region) → String property
     *   setIntProperty("retryCount", n)     → int property
     *   setBooleanProperty("urgent", true)  → boolean property
     *   JMS selector: SQL-92 syntax ile bu property'lere filtre uygular.
     *
     * İçeriğe göre routing (content-based routing) alternatifi:
     *   Mesaj içeriğini parse edip routing yapmak mümkün ama pahalı.
     *   Property: Header seviyesinde — broker body'i parse etmeden filtre uygular.
     *   Kural: Routing bilgisi header'da olmalı, iş verisi body'de.
     */
    public void sendWithProperties(OrderMessage order, String region, String tier) {
        jmsTemplate.convertAndSend(notificationQueue, order, message -> {
            message.setStringProperty("region", region);
            message.setStringProperty("tier", tier);
            message.setIntProperty("retryCount", order.getRetryCount());
            return message;
        });
    }

    /**
     * Request-Reply Deseni — Mesaj Üzerinde Senkron İletişim.
     *
     * sendAndReceive nedir?
     *   Normal gönderim: fire-and-forget — yanıt beklenmez.
     *   sendAndReceive: Mesaj gönderir, yanıt gelene kadar thread bloke olur.
     *   HTTP GET gibi davranır ama transport katmanı mesaj kuyruğudur.
     *
     * TemporaryQueue neden kullanılır?
     *   Her istek için benzersiz reply kuyruk oluşturulur.
     *   Consumer yanıtı bu kuyruğa gönderir.
     *   Bağlantı kapanınca TemporaryQueue otomatik silinir — temizlik gerekmez.
     *   Alternatif: Sabit reply queue + correlationId → birden fazla istek karışabilir.
     *
     * Ne zaman request-reply tercih edilir?
     *   Fiyat sorgulama: "Bu ürünün fiyatı nedir?" → yanıt anında gerekli.
     *   Onay sorgusu: "Bu işlem onaylandı mı?" → boolean yanıt bekleniyor.
     *   Dikkat: Mesaj kuyruğunun asıl amacı asenkroni — sendAndReceive bu avantajı kaldırır.
     *   Alternatif: HTTP/REST daha uygun olabilir — sendAndReceive nadiren kullanılmalı.
     */
    public String sendAndReceive(String payload) {
        Object reply = jmsTemplate.sendAndReceive(orderQueue, session -> {
            TextMessage msg = session.createTextMessage(payload);
            // TemporaryQueue: Bu istek için benzersiz yanıt kuyruğu
            msg.setJMSReplyTo(session.createTemporaryQueue());
            return msg;
        });
        return reply != null ? reply.toString() : null;
    }

    /**
     * DLQ'ya elle mesaj gönderimi — Yönetim / Test Aracı.
     *
     * Ne zaman kullanılır?
     *   Test: DLQ consumer'ını test etmek için — gerçek hata beklemeden DLQ dolu durum simüle et.
     *   Yönetim: Belirli bir mesajı manuel olarak işlenemez olarak işaretleme.
     *   Admin araçları: DLQ mesajlarını yönetim panelinden gönderme.
     *
     * dlq_reason property:
     *   DLQ'daki mesaj neden oraya düştü? Monitoring / alerting için kritik.
     *   Uyarı sistemi: DLQ'ya düşen her mesaj için alarm — operasyon ekibi bilgilendirilir.
     *   original_queue: Mesajın nereden geldiği bilinmezse tekrar işleme alınamaz.
     */
    public void sendToDlq(OrderMessage order, String reason) {
        jmsTemplate.convertAndSend("dead.letter.queue", order, message -> {
            message.setStringProperty("dlq_reason", reason);
            message.setStringProperty("original_queue", orderQueue); // Geri taşıma için
            return message;
        });
        log.warn("Message sent to DLQ — reason: {}, orderId: {}", reason, order.getOrderId());
    }

    /**
     * Toplu mesaj gönderimi — Batch Sending.
     *
     * Neden batch?
     *   Aynı işlem binlerce sipariş üretiyorsa tek tek gönderim yerine toplu gönderim.
     *   forEach: Her sipariş için ayrı sendOrder() → N ayrı JMS mesajı.
     *
     * Dikkat: Bu "gerçek" batch değil — her mesaj ayrı ayrı gönderilir.
     *   Gerçek batch (JDBC batch gibi): Tek network round-trip'te N mesaj.
     *   JMS batch: JMS standardında yoktur — N round-trip, N mesaj.
     *   Avantaj: Kod sadeleşir, logging tek yerden yapılır.
     *   Daha iyi alternatif: Kafka gibi sistemlerde gerçek batch gönderim mümkün.
     */
    public void sendBatch(java.util.List<OrderMessage> orders) {
        orders.forEach(this::sendOrder);
        log.info("Batch sent: {} messages", orders.size());
    }
}
