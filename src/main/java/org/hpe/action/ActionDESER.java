package org.hpe.action;

import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ActionDESER implements Serializer<Action>, Deserializer<Action> {

	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Action deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		Action action = null;
		try {
			action = mapper.readValue(data, Action.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return action;
	}


	@Override
	public byte[] serialize(String topic, Action action) {
		
		 byte[] result = null;
		 ObjectMapper objectMapper = new ObjectMapper();
		 try {
			 result = objectMapper.writeValueAsString(action).getBytes();
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
