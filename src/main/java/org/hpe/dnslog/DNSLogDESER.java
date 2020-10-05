package org.hpe.dnslog;

import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class DNSLogDESER implements Serializer<DNSLogEntry>, Deserializer<DNSLogEntry> {

	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public DNSLogEntry deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		DNSLogEntry dnsLogEntry = null;
		try {
			dnsLogEntry = mapper.readValue(data, DNSLogEntry.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dnsLogEntry;
	}


	@Override
	public byte[] serialize(String topic, DNSLogEntry data) {
		
		 byte[] result = null;
		 ObjectMapper objectMapper = new ObjectMapper();
		 try {
			 result = objectMapper.writeValueAsString(data).getBytes();
		 } catch (Exception e) {
		     e.printStackTrace();
		 }
		 return result;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
