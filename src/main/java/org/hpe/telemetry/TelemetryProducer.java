package org.hpe.telemetry;

import java.util.stream.IntStream;
import org.hpe.df.event.MessageProducer;
import org.hpe.dnslog.DNSLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryProducer {

	private static final Logger logger = LoggerFactory.getLogger(TelemetryProducer.class);
	
	
	public static void main(String[] args) throws Exception {	
		
		String KAFKA_INCOMING_TOPIC = "/sensor:metric";
		
		try ( 
			MessageProducer<DNSLogEntry,String,DNSLogEntry> producer = new MessageProducer.Builder<DNSLogEntry,String,DNSLogEntry>()
				.async(true)
				.topicName(KAFKA_INCOMING_TOPIC)
				.propsFile("/dnsLogProducer.props")
				.keyFunction( v -> v.getUid() )
				.valueFunction( v -> v )
				.build();				
		) {

			IntStream.range(0,10)
				.boxed()
				.forEach( v -> {
					DNSLogEntry dnsLogEntry = new DNSLogEntry();
					dnsLogEntry.setUid(String.format("device_%s", v) );
					dnsLogEntry.setTs(System.currentTimeMillis());
					
					producer.send(dnsLogEntry);
				});	
		}
		
		Thread.sleep(10000);
	}

}
