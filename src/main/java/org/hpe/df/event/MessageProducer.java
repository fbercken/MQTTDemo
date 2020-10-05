package org.hpe.df.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class MessageProducer<S,K,V> implements Closeable {
	
	private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
	
	private final String TOPIC;
	private final String PROPSFILE;
	private final boolean ASYNC;
	private final Function<S,K> buildKey;
	private final Function<S,V> buildValue;
	private BiConsumer<K,V> consumer;
	private KafkaProducer<K,V> producer;
	private final Class<? extends KafkaCallback<K,V>> CALLBACK;
	
	
	private MessageProducer(Builder<S,K,V> builder) {

		this.ASYNC = builder.async;
	    this.TOPIC = builder.topicName;
	    this.CALLBACK = builder.callback;
	    this.PROPSFILE = builder.propsFile;
	    this.buildKey = builder.keyFunction;
	    this.buildValue = builder.valueFunction;
		
		try (InputStream props = ClassLoader.class.getResourceAsStream(this.PROPSFILE)) {
			
			Properties properties = new Properties();
			properties.load(props);
			
			this.producer = new KafkaProducer<>(properties);
			this.consumer = setSender(producer);
			
		} catch(IOException e) {
			logger.error("unable to read properties file%s", e);
		}
	}
	
	
	public void send(S value) {
		try {
			consumer.accept( buildKey.apply(value), buildValue.apply(value) );
		} catch( Exception e) {
			throw new RuntimeException(e);
		}
	}

	

	@Override
	public void close() throws IOException {	
		if ( producer != null ) {
			producer.close();
		}	
	}
	
	
	private BiConsumer<K,V> setSender(final KafkaProducer<K,V> producer) {
		
		return (ASYNC) ?			
			(key,value) -> {
				try {
					KafkaCallback<K,V> callback = this.CALLBACK.getConstructor().newInstance();
					ProducerRecord<K,V> record = new ProducerRecord<>(this.TOPIC,key, value);
					producer.send( record, callback.setRecord(record));
					producer.flush();
				} catch (Exception e) {
					logger.error("Unable to send async message: %s", e);
				}
			}	
		:	
			(key,value) -> {
				try {
					producer.send( new ProducerRecord<>(this.TOPIC,key,value)).get();	
					producer.flush();
				} catch (InterruptedException | ExecutionException e) {
					logger.info( "Unable to send message: {}", e);
				}
			}
		;		
	}
	
	
	
	// Builder
	
	public static class Builder<S,K,V> {
		
		private boolean async;
		private String topicName;
		private String propsFile = "/producer.props";
		private Function<S,K> keyFunction;
		private Function<S,V> valueFunction;
		private Class<? extends KafkaCallback<K,V>> callback;
		
		
		public Builder<S,K,V> async(boolean async) {
			this.async = async;
			return this;
		}
		
		public Builder<S,K,V> topicName(String topicName) {
			this.topicName = topicName;
			return this;
		}
		
		public Builder<S,K,V> propsFile(String propsFile) {
			this.propsFile = propsFile;
			return this;
		}
		
		public Builder<S,K,V> callback(Class<? extends KafkaCallback<K,V>> callback) {
			this.callback = callback;
			return this;
		}
		
		public Builder<S,K,V> keyFunction(Function<S,K> keyFunction) {
			this.keyFunction = keyFunction;
			return this;
		}
		
		public Builder<S,K,V> valueFunction(Function<S,V> valueFunction) {
			this.valueFunction = valueFunction;
			return this;
		}
		
		public MessageProducer<S,K,V> build() {	
			return new MessageProducer<S,K,V>(this);
		}
		
	}
	
	

	

}
