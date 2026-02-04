package no.nav.kafka.sandbox.experiment;

import no.nav.kafka.sandbox.assignor.DynamicLagAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class ExperimentProducer {

    private final long rate; // сообщений в секунду
    private final int durationSeconds;
    private final String topic;
    private final boolean hotspotMode; // режим неоднородной нагрузки

    private Producer<String, String> producer;

    private static final Logger LOG = LoggerFactory.getLogger(DynamicLagAssignor.class);

    public ExperimentProducer(long rate, int durationSeconds, String topic, boolean hotspotMode) {
        this.rate = rate;
        this.durationSeconds = durationSeconds;
        this.topic = topic;
        this.hotspotMode = hotspotMode;
    }

    public void run() {
        LOG.info("Starting experiment producer: rate={}/sec, duration={}s, topic={}, hotspot={}",
                rate, durationSeconds, topic, hotspotMode);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer<>(props);
        Random random = new Random();
        AtomicLong sentMessages = new AtomicLong(0);

        long startTime = System.currentTimeMillis();
        long targetDuration = durationSeconds * 1000L;

        // Для неоднородной нагрузки: 80% трафика в 4 из 12 разделов
        int[] hotPartitions = hotspotMode ? new int[]{0, 1, 2, 3} : null;

        while (System.currentTimeMillis() - startTime < targetDuration) {
            long batchStart = System.currentTimeMillis();
            int batchSize = (int) (rate / 10); // 10 батчей в секунду

            for (int i = 0; i < batchSize; i++) {
                String key = String.valueOf(random.nextInt(1000));
                String value = "Message-" + System.currentTimeMillis() + "-" + random.nextInt(1000000);

                Integer partition = null;
                if (hotspotMode && random.nextDouble() < 0.8) {
                    // 80% сообщений в горячие разделы
                    partition = hotPartitions[random.nextInt(hotPartitions.length)];
                }

                producer.send(new ProducerRecord<>(topic, partition, key, value),
                        (metadata, exception) -> {
                            if (exception != null) {
                                LOG.error("Send error: {}", exception.getMessage());
                            }
                        });

                sentMessages.incrementAndGet();
            }

            producer.flush();

            // Поддержание скорости
            long elapsed = System.currentTimeMillis() - batchStart;
            if (elapsed < 100) {
                try {
                    Thread.sleep(100 - elapsed);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        producer.close();
        LOG.info("Producer finished. Total sent: {}", sentMessages.get());
    }
}