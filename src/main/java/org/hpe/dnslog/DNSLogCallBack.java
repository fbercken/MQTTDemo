package org.hpe.dnslog;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.hpe.df.event.KafkaCallback;

import com.fasterxml.jackson.databind.ObjectMapper;


public class DNSLogCallBack extends KafkaCallback<String,byte[]> {
	
	
	private static final Logger logger = LoggerFactory.getLogger(DNSLogCallBack.class);

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
	
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			DNSLogEntry logEntry = objectMapper.readValue( record.value(), DNSLogEntry.class);
			logger.info("Submitted: key {}: value {}", record.key(), objectMapper.writeValueAsString(logEntry));
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}	

}
