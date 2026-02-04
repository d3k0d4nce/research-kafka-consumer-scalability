package no.nav.kafka.sandbox.assignor;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicLagAssignor extends AbstractPartitionAssignor {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicLagAssignor.class);

    @Override
    public String name() {
        return "dynamic-lag";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String, Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions) {

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        List<String> consumers = new ArrayList<>(subscriptions.keySet());

        // Простое RoundRobin распределение для начала
        int consumerIndex = 0;
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int numPartitions = topicEntry.getValue();

            for (int partition = 0; partition < numPartitions; partition++) {
                TopicPartition tp = new TopicPartition(topic, partition);
                String consumer = consumers.get(consumerIndex % consumers.size());
                assignment.computeIfAbsent(consumer, k -> new ArrayList<>())
                        .add(tp);
                consumerIndex++;
            }
        }

        LOG.info("DynamicLagAssignor assigned partitions to {} consumers", consumers.size());
        return assignment;
    }
}