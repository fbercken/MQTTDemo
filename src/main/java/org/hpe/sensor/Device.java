package org.hpe.sensor;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.hpe.action.Action;
import org.hpe.dnslog.DNSLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

public class Device {
	
	private static final Logger logger = LoggerFactory.getLogger(Device.class);
	
	private String deviceId;
	private Mqtt5AsyncClient mqttClient;


	public Device() {
		
		deviceId = "client_" + UUID.randomUUID();
				
		mqttClient = Mqtt5Client.builder()
	        .identifier(deviceId)
	        .serverHost("localhost")
	        .serverPort(1883)
	        .automaticReconnectWithDefaultConfig() 
	        .buildAsync();	
	}
	
	private void connect(String user, String password) {
		
		mqttClient.toBlocking().connectWith()
	        .simpleAuth()
	        .username(user)
	        .password(password.getBytes())
	        .applySimpleAuth() 
	        .send();
	}
	
	private void publish(String topic, String message) {
		
		mqttClient.publishWith()
	        .topic(topic)
	        .payload(message.getBytes(StandardCharsets.UTF_8))
	        .send()
	        .whenComplete((publish, throwable) -> {
	            if (throwable != null) {
	            	logger.error("Publish failed: {}", throwable);
	            } else {
	            	logger.info("Publish: {}", message);
	            }
	        });
	}
	
	private void subscribe(String topic, Consumer<Mqtt5Publish> mqttConsumer) {
		
		mqttClient.subscribeWith()
	        .topicFilter(topic)
	        .callback(mqttConsumer)
	        .send()
	        .whenComplete((subAck, throwable) -> {
	            if (throwable != null) {
	            	logger.error("Subscription to {} failed: {}", topic, throwable );
	            } else {
	            	logger.debug("Subscription established: {}", topic);
	            }
	        });
	}
	
	
	
	public static void main(String[] args) {
		
		Device device = new Device();
		device.connect( "fberque", "password");
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		device.subscribe("/action", mqtt5Publish -> {	
			Action action;
			try {
				action = objectMapper.readValue( mqtt5Publish.getPayloadAsBytes(), Action.class);
				logger.info( "Topic: {}, Received: {}", mqtt5Publish.getTopic(), action.getName() );
			} catch (Exception e) {
				e.printStackTrace();
			} 
		});		
		
			
		IntStream.range(0,10).boxed().forEach( v -> {
			try {
				
				DNSLogEntry dnsLogEntry = new DNSLogEntry();
				dnsLogEntry.setUid(device.deviceId);
				dnsLogEntry.setOrigin_h("102.168.0.1");
				dnsLogEntry.setOrigin_p("8080");
				dnsLogEntry.setResp_h("102.168.0.2");
				dnsLogEntry.setResp_p("8080");
				dnsLogEntry.setTs( System.currentTimeMillis());

				device.publish( "/sensor", objectMapper.writeValueAsString(dnsLogEntry) );
				
			} catch (JsonProcessingException e) {
				logger.error("Error: {}", e);
			}
			
		});
		
	}

}
