package org.hpe.telemetry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hpe.df.database.DocLoader;
import org.hpe.df.event.MessageConsumer;
import org.hpe.dnslog.DNSLogEntry;

import java.util.concurrent.atomic.AtomicInteger;


public class TelemetryConsumer {
	
	private static final Logger logger = LoggerFactory.getLogger(TelemetryConsumer.class);

	
	
	public static void main(String[] args) {
		
		final String TABLENAME = "/metrics";
		final String KAFKA_INCOMING_TOPIC = "/sensor:metric";
		final AtomicInteger counter = new AtomicInteger(0);
		
		try ( 
				
			DocLoader<DNSLogEntry> docLoader = new DocLoader.Builder<DNSLogEntry>()
				.storeName(TABLENAME)
				.keyFunction( v -> String.valueOf(counter.incrementAndGet()) )
				.documentFunction( (conn, v) -> {
					return conn.newDocument()
						//.setId( String.valueOf(counter.incrementAndGet())  )   //String.format("%s-%s", v.getTimestamp(), v.getDeviceId()) )
						.set("uid", v.getUid())
						.set("timestamp", v.getTs())
						.set("originIP", v.getOrigin_h())
						.set("originPort", v.getOrigin_p())
						.set("destinationIP",v.getResp_h())
						.set("destinationPort",v.getResp_p());
				})
				.build();	
			
			MessageConsumer<String,DNSLogEntry> consumer = new MessageConsumer.Builder<String,DNSLogEntry>()
				.pooling(100)
				.topicName(KAFKA_INCOMING_TOPIC)	
				.propsFile("/dnsLogConsumer.props")
				.consumerFunction( message -> docLoader.insert(message.value()) )
				.build();
						
		) {
		
			consumer.run();
			
		} catch (Exception e) {
			logger.error("Initialization error: %s", e);
		}

	}

}
