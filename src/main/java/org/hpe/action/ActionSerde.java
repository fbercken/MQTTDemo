package org.hpe.action;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ActionSerde implements Serde<Action> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Serializer<Action> serializer() {
		// TODO Auto-generated method stub
		return new ActionDESER();
	}

	@Override
	public Deserializer<Action> deserializer() {
		// TODO Auto-generated method stub
		return new ActionDESER();
	}

}
