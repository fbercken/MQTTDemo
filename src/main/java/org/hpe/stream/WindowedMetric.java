package org.hpe.stream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.Topology;
import org.hpe.action.Action;
import org.hpe.action.ActionSerde;
import org.hpe.dnslog.DNSLogEntry;
import org.hpe.dnslog.DNSLogSerde;

public class WindowedMetric {
	
	final long THRESHOLD = 2;
	final String APP_ID = "windowApp";
	final String INPUT_TOPIC = "/sensor:metric";
	final String OUTPUT_TOPIC = "/sensor:action";
	final String AlERT_TOPIC = "/alert:devices";
	final String DEFAULT_STREAM = "/sample-stream";
	
	
	
	public Properties buildProperties() {
		
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500); 
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,LongSerde.class); 
      //  props.put(StreamsConfig.STREAMS_DEFAULT_STREAM_CONFIG, DEFAULT_STREAM);
 
        return props;
	}
	
	
	public Topology buildTopology(TimeWindows timeWindow) {
		
		StreamsBuilder builder = new StreamsBuilder();			
		Consumed.with(new DNSLogTimestampExtractor());
		
		KStream<String,Action> stream = builder.stream( INPUT_TOPIC, Consumed.with(Serdes.String(),new DNSLogSerde())) 	
			.map( (key,dnsLog) -> new KeyValue<>( dnsLog.getUid(), 1L) )
			.groupByKey()
			.windowedBy(timeWindow)
			.count()
			.toStream()
			
		//	.process(processorSupplier, stateStoreNames)   Need to add state store based on DF
			.filter( (key,value) -> (value > THRESHOLD) )
			.map( ( key, count) -> {
				Action action = new Action();
				action.setName( "Do something " + count.toString());
				action.setDeviceId(key.key());
				return new KeyValue<>( key.key(), action);
			});

		//stream.to(AlERT_TOPIC, Produced.with( Serdes.String(), new ActionSerde()) );			
		stream.to(OUTPUT_TOPIC, Produced.with( Serdes.String(), new ActionSerde()) );
			
		return builder.build();	
	}
	
	
	public static void main(String[] args)  {
		
		WindowedMetric app = new WindowedMetric();
	      
        long windowSizeMs = TimeUnit.SECONDS.toMillis(5);
        TimeWindows timeWindow = TimeWindows.of(windowSizeMs).advanceBy(windowSizeMs);
        
        
        Properties props = app.buildProperties();
        Topology topology = app.buildTopology(timeWindow);
        
		KafkaStreams streams = new KafkaStreams(topology, props);
       
		CountDownLatch latch = new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
	}
	
	
	
	public class DNSLogTimestampExtractor implements TimestampExtractor {

		@Override
		public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
			
			return ((DNSLogEntry) record.value()).getTs();
		}
	}

}
