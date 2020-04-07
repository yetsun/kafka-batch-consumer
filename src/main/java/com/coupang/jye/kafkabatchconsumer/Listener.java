package com.coupang.jye.kafkabatchconsumer;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class Listener implements ConsumerSeekAware {

	private static final Logger LOG = LoggerFactory.getLogger(Listener.class);

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@KafkaListener(id = "batch-listener", topics = "${app.topic.batch}")
	public void receive(@Payload List<String> messages,
		@Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
		@Header(KafkaHeaders.OFFSET) List<Long> offsets) {

		LOG.info("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
		LOG.info("beginning to consume batch messages");

		for (int i = 0; i < messages.size(); i++) {

			LOG.info("received message='{}' with partition-offset='{}'",
				messages.get(i), partitions.get(i) + "-" + offsets.get(i));

		}
		LOG.info("all batch messages consumed");

		stopConsumingKafka();
	}

	private void stopConsumingKafka() {
		LOG.info("before stop");
		MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("batch-listener");
		listenerContainer.stop();
		LOG.info("after stop");
	}

	/**
	 * Rest offset to end -5
	 */
	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		//reset offset to latest
		//        assignments.forEach((t, o) -> callback.seekToEnd(t.topic(), t.partition()));
		assignments.forEach((t, o) -> callback.seekRelative(t.topic(), t.partition(), -5, false));
	}

}