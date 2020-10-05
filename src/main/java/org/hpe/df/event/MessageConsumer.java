package org.hpe.df.event;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class MessageConsumer<K,V> implements Closeable  {

	private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
	
	private final String TOPIC;
	private final int POOLING;
	private final String PROPSFILE;
	private KafkaConsumer<K,V> CONSUMER;
	private Consumer<ConsumerRecord<K,V>> CONSUMERFUNCTION;
	
	
	private MessageConsumer(Builder<K,V> builder) {
		 
    	this.TOPIC = builder.topicName;
    	this.PROPSFILE = builder.propsFile;
    	this.POOLING = builder.pooling;
    	this.CONSUMERFUNCTION = builder.consumerFunction;
		
		try (InputStream props = ClassLoader.class.getResourceAsStream(this.PROPSFILE) ) {
			
			Properties properties = new Properties();
            properties.load(props);
            
            this.CONSUMER = new KafkaConsumer<>(properties);
            CONSUMER.subscribe( Arrays.asList(TOPIC));
           
		} catch( IOException e) {			
			logger.error("Unable to initialize consumer: %s", e);
		} 
	}
	
	
	@Override
	public void close() throws IOException {
		if ( CONSUMER != null) {
			CONSUMER.close();
		}	
	}
	
	
	public void run() {
		
		ConsumerRecords<K,V> records;
		
		while (true) {				
           records = CONSUMER.poll(POOLING);	
           for (ConsumerRecord<K,V> record : records) {
        	   CONSUMERFUNCTION.accept(record);
           }
		}
	}
	
	
	
	// Builder
	
		public static class Builder<K,V> {
			
			private int pooling;
			private String topicName;
			private String propsFile = "/consumer.props";
			private Consumer<ConsumerRecord<K,V>> consumerFunction;
			
			
			public Builder<K,V> pooling(int pooling) {
				this.pooling = pooling;
				return this;
			}
			
			public Builder<K,V> topicName(String topicName) {
				this.topicName = topicName;
				return this;
			}
			
			public Builder<K,V> propsFile(String propsFile) {
				this.propsFile = propsFile;
				return this;
			}
			
			public Builder<K,V> consumerFunction(Consumer<ConsumerRecord<K,V>> consumerFunction) {
				this.consumerFunction = consumerFunction;
				return this;
			}
			
			public MessageConsumer<K,V> build() {	
				return new MessageConsumer<K,V>(this);
			}
			
		}

}