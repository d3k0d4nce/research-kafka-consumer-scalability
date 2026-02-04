package no.nav.kafka.sandbox.experiment;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ExperimentConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExperimentConsumer.class);

    private final String consumerId;
    private final String strategy;
    private final int sessionTimeout;
    private final int heartbeatInterval;
    private final String groupId;
    private final String topic;

    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final Map<String, List<Long>> latencyHistory = new ConcurrentHashMap<>();
    private Consumer<String, String> consumer;
    private volatile boolean running = true;  // ← ДОБАВЛЕНО: флаг работы
    private Thread consumerThread;  // ← ДОБАВЛЕНО: ссылка на поток

    public ExperimentConsumer(String consumerId, String strategy,
                              int sessionTimeout, int heartbeatInterval,
                              String groupId, String topic) {
        this.consumerId = consumerId;
        this.strategy = strategy;
        this.sessionTimeout = sessionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.groupId = groupId;
        this.topic = topic;
    }

    public void start() {
        log.info("Starting experiment consumer {}: strategy={}, group={}, topic={}",
                consumerId, strategy, groupId, topic);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        // Настройка стратегии распределения
        switch (strategy.toLowerCase()) {
            case "roundrobin":
                props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        "org.apache.kafka.clients.consumer.RoundRobinAssignor");
                break;
            case "sticky":
                props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        "org.apache.kafka.clients.consumer.StickyAssignor");
                break;
            case "dynamic":
                props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        "no.nav.kafka.sandbox.assignor.DynamicLagAssignor");
                break;
            default: // range
                props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        "org.apache.kafka.clients.consumer.RangeAssignor");
        }

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("Consumer {} lost partitions: {}", consumerId, partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Consumer {} assigned partitions: {}", consumerId, partitions);
            }
        });

        // Запуск цикла обработки с сохранением ссылки на поток
        consumerThread = new Thread(this::consumeLoop, "consumer-" + consumerId);
        consumerThread.start();

        // Добавляем shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook called for consumer {}", consumerId);
            stop();
        }));
    }

    private void consumeLoop() {
        Random random = new Random();

        try {
            while (running && !Thread.currentThread().isInterrupted()) {  // ← ИСПРАВЛЕНО: условие выхода
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        long receiveTime = System.currentTimeMillis();

                        // Симуляция обработки (10-50 мс)
                        Thread.sleep(random.nextInt(40) + 10);

                        long latency = System.currentTimeMillis() - receiveTime;
                        String secondKey = String.valueOf(receiveTime / 1000);
                        latencyHistory.computeIfAbsent(secondKey, k -> new ArrayList<>())
                                .add(latency);

                        messagesProcessed.incrementAndGet();

                        // Периодический лог
                        if (messagesProcessed.get() % 100 == 0) {
                            log.debug("Consumer {} processed {} messages", consumerId, messagesProcessed.get());
                        }
                    }

                    consumer.commitSync();

                } catch (WakeupException e) {
                    // Это нормально при вызове wakeup()
                    log.info("Consumer {} woken up", consumerId);
                    break;
                } catch (InterruptedException e) {
                    log.info("Consumer {} interrupted", consumerId);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in consumer {}: {}", consumerId, e.getMessage());
                    // При критической ошибке выходим
                    if (e.getCause() instanceof InterruptedException) {
                        break;
                    }
                }
            }
        } finally {
            // Гарантированно закрываем consumer
            if (consumer != null) {
                try {
                    consumer.close();
                    log.info("Consumer {} closed Kafka consumer", consumerId);
                } catch (Exception e) {
                    log.error("Error closing consumer {}: {}", consumerId, e.getMessage());
                }
            }
            log.info("Consumer {} stopped. Total messages: {}", consumerId, messagesProcessed.get());
        }
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("consumerId", consumerId);
        metrics.put("messagesProcessed", messagesProcessed.get());
        metrics.put("strategy", strategy);

        // Расчет перцентилей задержек
        List<Long> allLatencies = new ArrayList<>();
        latencyHistory.values().forEach(allLatencies::addAll);

        if (!allLatencies.isEmpty()) {
            Collections.sort(allLatencies);
            metrics.put("p50", allLatencies.get((int) (allLatencies.size() * 0.5)));
            metrics.put("p95", allLatencies.get((int) (allLatencies.size() * 0.95)));
            metrics.put("p99", allLatencies.get((int) (allLatencies.size() * 0.99)));
        }

        return metrics;
    }

    public void stop() {
        log.info("Stopping consumer {}", consumerId);
        running = false;  // ← ИСПРАВЛЕНО: устанавливаем флаг

        if (consumer != null) {
            consumer.wakeup();  // ← Вызываем WakeupException в poll()
        }

        if (consumerThread != null && consumerThread.isAlive()) {
            consumerThread.interrupt();  // ← Прерываем поток
            try {
                consumerThread.join(5000);  // ← Ждём завершения 5 секунд
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}