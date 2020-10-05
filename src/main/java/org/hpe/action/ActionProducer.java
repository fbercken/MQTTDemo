package org.hpe.action;

import java.util.stream.IntStream;

import org.hpe.df.event.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ActionProducer {

	private static final Logger logger = LoggerFactory.getLogger(ActionProducer.class);
	
	
	public static void main(String[] args) throws Exception {		
		
		try ( 
			MessageProducer<Action,String,Action> producer = new MessageProducer.Builder<Action,String,Action>()
				.async(true)
				.topicName("/sensor:action")
				.propsFile("/actionProducer.props")
				.callback( ActionCallback.class)
				.keyFunction( v -> v.getDeviceId() )
				.valueFunction( v -> v )
				.build();				
		) {

			IntStream.range(0,10)
				.boxed()
				.forEach( v -> {
					Action action = new Action();
					action.setDeviceId(String.format("device_%s", v) );
					action.setName(String.format( "do something %s", v));
					action.setTimestamp(System.currentTimeMillis());
					
					producer.send(action);
				});	
		}
		
		Thread.sleep(10000);
	}
	


}
