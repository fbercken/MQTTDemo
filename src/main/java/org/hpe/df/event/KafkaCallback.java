package org.hpe.df.event;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaCallback<K,V> implements Callback {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaCallback.class);
	
	protected ProducerRecord<K,V> record;
	
	
	public KafkaCallback() {
		
	}

	public KafkaCallback<K,V> setRecord(ProducerRecord<K,V> record) {
		this.record = record;
		return this;
	}
	

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		logger.info("Submitted {}: {}", record.key(), record.value());		
	}		
}