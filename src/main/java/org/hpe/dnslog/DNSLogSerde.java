package org.hpe.dnslog;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class DNSLogSerde implements Serde<DNSLogEntry> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<DNSLogEntry> serializer() {
		// TODO Auto-generated method stub
		return new DNSLogDESER();
	}

	@Override
	public Deserializer<DNSLogEntry> deserializer() {
		// TODO Auto-generated method stub
		return new DNSLogDESER();
	}

}
