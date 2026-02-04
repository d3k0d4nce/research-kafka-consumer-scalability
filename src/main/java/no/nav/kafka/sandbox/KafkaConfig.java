package no.nav.kafka.sandbox;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Common Kafka config for producer/consumers.
 */
class KafkaConfig {

    /**
     * See <a href="http://kafka.apache.org/documentation.html#producerconfigs">Producer configs</a>
     */
    static Map<String,Object> kafkaProducerProps() {
        var props = new HashMap<String,Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.DEFAULT_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);  // Request timeout, default is 30 seconds
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);               // At default value of 0, affects batching of messages
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);          // at default value of 16384 bytes
        props.put(ProducerConfig.ACKS_CONFIG, "1");                  // Require ack from leader only, at default value
        return props;
    }

    /**
     * See <a href="http://kafka.apache.org/documentation.html#consumerconfigs">Consumer configs</a>
     */
    static Map<String,Object> kafkaConsumerProps(String groupId) {
        var props = new HashMap<String,Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.DEFAULT_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        return props;
    }
}
