package ogr.hpe.bridge;

import java.util.UUID;
import java.util.function.Consumer;

import org.hpe.df.event.MessageConsumer;
import org.hpe.df.event.MessageProducer;
import org.hpe.dnslog.DNSLogCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

public class MQTTBridge {
	
	
private static final Logger logger = LoggerFactory.getLogger(MQTTBridge.class);
	
	private String bridgeId;
	private Mqtt5AsyncClient mqttClient;


	public MQTTBridge() {
		
		bridgeId = "bridge_" + UUID.randomUUID();
				
		mqttClient = Mqtt5Client.builder()
	        .identifier(bridgeId)
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
	
	private void publish(String topic, byte[] message) {
		
		mqttClient.publishWith()
	        .topic(topic)
	        .payload(message)
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
	
	
	public static void main(String[] args) throws Exception {

		MQTTBridge bridge = new MQTTBridge();
		
		String MQTT_TOPIC = "/sensor";
		String KAFKA_INCOMING_TOPIC = "/sensor:metric";
		String KAFKA_OUTGOING_TOPIC = "/sensor:action";
		
		
		bridge.connect( "fberque", "password");
		
		try ( 
			MessageProducer<byte[],String,byte[]> producer = new MessageProducer.Builder<byte[],String,byte[]>()
				.async(true)
				.topicName(KAFKA_INCOMING_TOPIC)
				.propsFile("/byteArrayProducer.props")
				.callback( DNSLogCallBack.class)
				.keyFunction( v -> "" )
				.valueFunction( v -> v )
				.build();	
			
			MessageConsumer<String,byte[]> consumer = new MessageConsumer.Builder<String,byte[]>()
				.pooling(100)
				.topicName(KAFKA_OUTGOING_TOPIC)
				.propsFile("/byteArrayConsumer.props")
				.consumerFunction( message -> bridge.publish("/action", message.value() ) )
				.build();					
				
		) {
			
			bridge.subscribe( MQTT_TOPIC, mqtt5Publish -> producer.send( mqtt5Publish.getPayloadAsBytes()) );

			consumer.run();
		}

	}
	

}
