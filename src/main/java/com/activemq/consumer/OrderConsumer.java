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
 * JMS MESAJ TÜKETİCİSİ — ActiveMQ Consumer Desenleri
 * ====================================================
 *
 * JMS (Java Message Service): Mesaj aracısı ile iletişim için Java standardı.
 * ActiveMQ bu standardı uygular — producer mesaj koyar, consumer alır.
 *
 * Queue vs Topic farkı:
 *   Queue (Point-to-Point): Mesaj tam olarak BİR consumer'a iletilir.
 *     → Sipariş işleme: Her sipariş yalnızca bir kez işlenmeli.
 *   Topic (Publish-Subscribe): Mesaj TÜM subscriber'lara iletilir.
 *     → Stok güncellemesi: Hem sipariş servisi hem bildirim servisi bilgilendirilmeli.
 *
 * containerFactory neden belirtilir?
 *   Queue ve Topic farklı konfigürasyon gerektirir (pubSubDomain true/false).
 *   queueListenerFactory: pubSubDomain=false → Queue modu.
 *   topicListenerFactory: pubSubDomain=true → Topic modu.
 *   Yanlış factory: Queue consumer Topic'i veya Topic consumer Queue'yu dinleyemez.
 *
 * Bu sınıftaki desenler:
 *   processOrder         → Transactional queue consumer (temel desen)
 *   processEuPremiumNotification → Property selector ile broker-tarafı filtreleme
 *   handleInventoryUpdate → Durable topic subscriber (kesinti toleransı)
 *   processWithRetryAndDlq → Manuel DLQ yönetimi (max retry)
 *   replyHandler          → Request-Reply deseni (korelasyon ID'si)
 */
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    // Kaç kez yeniden deneneceği — daha fazlası DLQ'ya
    private static final int MAX_RETRIES = 3;

    private final JmsTemplate jmsTemplate;

    @Value("${app.queues.dlq}")
    private String dlqDestination;

    public OrderConsumer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    /**
     * Temel Queue consumer — Transactional Acknowledgement.
     *
     * @JmsListener nasıl çalışır?
     *   Spring, arka planda bir MessageListenerContainer başlatır.
     *   Container, broker'a bağlanır ve bu kuyruğu sürekli dinler.
     *   Mesaj gelince: JSON/Object deserialize edilir → bu metod çağrılır.
     *
     * sessionTransacted=true (queueListenerFactory konfigürasyonunda):
     *   Normal akış: Metod başarıyla döner → broker mesajı ACK (teslim alındı) kabul eder.
     *   Exception fırlatılırsa: Broker NACK (teslim alınmadı) kabul eder → mesajı tekrar kuyruğa koyar.
     *   Neden throw e? "throw e" olmadan exception yutulur → broker ACK sayar → mesaj kaybolur.
     *   Bu sayede işlem başarısız olursa mesaj otomatik yeniden iletilir.
     *
     * Broker redelivery'den sonra ne olur?
     *   Broker varsayılan olarak belirli sayıda (6 kez) yeniden dener.
     *   Tüm denemeler başarısız → ActiveMQ.DLQ kuyruğuna taşır (Dead Letter Queue).
     */
    @JmsListener(destination = "${app.queues.order}", containerFactory = "queueListenerFactory")
    public void processOrder(OrderMessage order) {
        log.info("Processing order: {}", order.getOrderId());
        try {
            doProcessOrder(order);
            log.info("Order processed successfully: {}", order.getOrderId());
        } catch (Exception e) {
            log.error("Order processing failed: {} — {}", order.getOrderId(), e.getMessage());
            // sessionTransacted=true → exception = NACK → broker mesajı yeniden kuyruğa koyar
            throw e;
        }
    }

    private void doProcessOrder(OrderMessage order) {
        // İş kuralı doğrulaması: amount sıfır veya negatifse işlenemez
        if (order.getAmount() == null || order.getAmount().signum() <= 0) {
            throw new IllegalArgumentException("Invalid amount: " + order.getAmount());
        }
        order.setStatus("CONFIRMED");
        log.info("Order confirmed: {} — amount: {}", order.getOrderId(), order.getAmount());
    }

    /**
     * Property Selector ile broker-tarafı filtreleme.
     *
     * selector = "region = 'EU' AND tier = 'PREMIUM'" ne yapar?
     *   SQL-92 sözdizimi ile JMS property'lerine filtre uygular.
     *   Broker, mesajı bu consumer'a yalnızca koşullar sağlanıyorsa iletir.
     *   Diğer mesajlar (region=TR, tier=BASIC) başka consumer'lara gider.
     *
     * Neden client-tarafı filtre yerine broker filtresi?
     *   Client filtresi: Tüm mesajlar iletilir, consumer if/else ile ayıklar.
     *   Broker filtresi: Gereksiz mesajlar ağ üzerinden consumer'a hiç gelmez.
     *   Büyük hacimde mesaj varsa: Ağ trafiği ve CPU tasarrufu önemli.
     *   Kural: İşlenmeyecek mesajlar consumer'a gelmemeli — broker seviyesinde filtrele.
     *
     * rawMessage neden lazım?
     *   OrderMessage (deserialize edilmiş nesne): İş verisini taşır.
     *   Message (ham JMS mesajı): JMS header ve property'lere erişim.
     *   Property'ler header'da (region, tier) — business nesnesinde değil.
     */
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

    /**
     * Durable Topic Subscriber — Kesinti Toleranslı Abonelik.
     *
     * Neden durable (kalıcı) subscription?
     *   Non-durable topic: Consumer offline iken gelen mesajlar kaybolur.
     *   Durable topic: Broker, subscriber offline iken gelen mesajları saklar.
     *   Consumer tekrar ayağa kalktığında birikmiş mesajlar iletilir.
     *
     * Ne zaman durable gerekir?
     *   Stok güncellemesi: inventory-service 5 dakika yeniden başlıyorsa,
     *   bu süredeki stok değişikliklerini kaçırmamalı.
     *   E-posta bildirimi: Email servisi geçici düşerse, mesajlar sonra işlenmeli.
     *
     * subscription = "inventory-service-sub" neden önemli?
     *   Broker, bu isimle kaydı tutar — hangi mesajın hangi subscriber'a iletildiğini izler.
     *   Aynı isimle yeniden bağlanan consumer = aynı abonelik = birikmiş mesajlar teslim edilir.
     *   Farklı isim = yeni abonelik = önceki mesajlar kaybolur.
     *
     * Map<String, Object> parametre neden?
     *   Stok olayı basit bir veri yapısı — ayrı bir class gerekmiyor.
     *   Jackson: JSON → Map dönüşümünü otomatik yapar.
     */
    @JmsListener(
            destination = "${app.topics.inventory}",
            containerFactory = "topicListenerFactory",
            subscription = "inventory-service-sub"
    )
    public void handleInventoryUpdate(java.util.Map<String, Object> event) {
        log.info("Inventory update received: product={}, stock={}",
                event.get("productId"), event.get("newStock"));
    }

    /**
     * Manuel DLQ (Dead Letter Queue) Yönetimi.
     *
     * DLQ nedir ve neden gerekli?
     *   Bazı mesajlar iş mantığı hatası nedeniyle HİÇBİR ZAMAN işlenemez.
     *   Örnek: Bozuk veri, kayıp referans, iş kuralı her seferinde ihlal edilmesi.
     *   Sonsuz döngü: broker → consumer (hata) → broker → consumer (hata) ...
     *   DLQ: Belirli retry sonrası "işlenemeyen" mesajlar buraya alınır.
     *   DLQ sayesinde: Sorunlu mesajlar sistemi bloke etmez, inceleme için ayrılır.
     *
     * JMSXDeliveryCount: Broker tarafından set edilen standart JMS property.
     *   1: İlk teslimat
     *   2: Birinci yeniden deneme
     *   3: İkinci yeniden deneme
     *   MAX_RETRIES (3) aşılırsa: DLQ'ya taşı, mesajı ack'le (artık yeniden denenmesin).
     *
     * Manuel DLQ vs Otomatik DLQ farkı?
     *   Otomatik: Broker kendi kendine DLQ'ya taşır — ama hangi DLQ, neden bilgisi yok.
     *   Manuel: Consumer DLQ'ya taşır → reason property eklenebilir → monitoring/alerting zengin.
     *   Tercih: Manuel — neden DLQ'ya düştüğü izlenebilir (operasyonel görünürlük).
     */
    @JmsListener(destination = "${app.queues.order}", containerFactory = "queueListenerFactory")
    public void processWithRetryAndDlq(OrderMessage order, Message rawMessage) throws Exception {
        int retryCount = rawMessage.getIntProperty("JMSXDeliveryCount");
        log.info("Processing with retry — orderId: {}, attempt: {}", order.getOrderId(), retryCount);

        if (retryCount > MAX_RETRIES) {
            log.error("Max retries exceeded — sending to DLQ: {}", order.getOrderId());
            // DLQ'ya taşı: neden düştüğünü ve kaç kez denendiğini property olarak ekle
            jmsTemplate.convertAndSend(dlqDestination, order, msg -> {
                msg.setStringProperty("dlq_reason", "max_retries_exceeded");
                msg.setIntProperty("original_retry_count", retryCount);
                return msg;
            });
            return; // return: mesajı ack'le — broker tekrar denemesin (DLQ'ya zaten gönderildi)
        }

        processOrder(order);
    }

    /**
     * Request-Reply Deseni — Asenkron Transport Üzerinde Senkron Yanıt.
     *
     * Request-Reply ne zaman kullanılır?
     *   Normal pub-sub: Producer gönderir, yanıt beklemez (fire-and-forget).
     *   Request-Reply: Producer yanıt bekler — ama HTTP değil, mesaj aracısı üzerinden.
     *   Kullanım: Sipariş fiyat hesaplama, onay sorgusu gibi yanıt gerektiren işlemler.
     *
     * JMSReplyTo:
     *   Producer'ın yanıtın geleceği kuyruğu belirtir (genellikle TemporaryQueue).
     *   TemporaryQueue: Bağlantı kapanınca otomatik silinir — tek kullanımlık.
     *   Consumer bu property'den reply queue adresini okur ve yanıtı oraya gönderir.
     *
     * JMSCorrelationID:
     *   Producer birden fazla istek gönderirse hangi yanıtın hangi isteğe ait olduğu bilinmeli.
     *   Producer: correlationId set eder (genellikle JMSMessageID kopyalanır).
     *   Consumer: Yanıtta aynı correlationId'yi set eder.
     *   Producer: correlationId ile kendi yanıtını eşleştirir.
     */
    @JmsListener(destination = "${app.queues.order}", containerFactory = "queueListenerFactory")
    public void replyHandler(TextMessage request) throws Exception {
        String payload = request.getText();
        jakarta.jms.Destination replyTo = request.getJMSReplyTo();
        String correlationId = request.getJMSCorrelationID();

        // replyTo null kontrolü: Yanıt beklenmeyen (fire-and-forget) mesajlar da gelebilir
        if (replyTo != null) {
            String response = "PROCESSED: " + payload;
            // Yanıtı reply-to kuyruğuna gönder — correlationId ile eşleştirme sağlanır
            jmsTemplate.send(replyTo, session -> {
                TextMessage reply = session.createTextMessage(response);
                reply.setJMSCorrelationID(correlationId); // Producer bu ID ile yanıtı bulur
                return reply;
            });
        }
    }
}
