package org.hpe.action;

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ActionSerde implements Serde<Action> {
		
	private ActionDESER actionDESER;  
	
	public ActionSerde() {
		this.actionDESER = new ActionDESER();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.actionDESER.configure(configs, isKey);
	}

	@Override
	public void close() {
		this.actionDESER.close();		
	}

	@Override
	
	public Serializer<Action> serializer() {
		return this.actionDESER;
	}

	@Override
	public Deserializer<Action> deserializer() {
		return this.actionDESER;
	}

}
