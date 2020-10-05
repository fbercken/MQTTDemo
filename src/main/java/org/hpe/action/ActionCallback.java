package org.hpe.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hpe.df.event.KafkaCallback;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;


public class ActionCallback extends KafkaCallback<String,Action> {
	
	private static final Logger logger = LoggerFactory.getLogger(ActionCallback.class);
		
	public ActionCallback() {
	
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
	
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			logger.info("Action: {} / {}", record.key(), objectMapper.writeValueAsString(record.value()) );
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}	

}
