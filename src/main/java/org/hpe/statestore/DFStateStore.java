package org.hpe.statestore;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class DFStateStore<K,V> implements KeyValueStore<K,V>  {
	
	private boolean open = false;
	private String topic;
	private String storeName;
	private DocumentStore store;
	private Connection connection;

	protected Serializer<K> keySerializer;
	protected Deserializer<K> keyDeserializer;
	protected Serializer<V> valueSerializer;
	protected Deserializer<V> valueDeserializer;


	@Override
	public String name() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init(ProcessorContext context, StateStore root) {
		
		this.open = true;
		this.topic = context.topic();
		this.storeName = (String) context.appConfigs().get("table");
		this.connection = DriverManager.getConnection("ojai:mapr:");		
		this.store = connection.storeExists(storeName)? connection.getStore(storeName) : connection.createStore(storeName);	
		
		this.keySerializer = (Serializer<K>)  context.keySerde().serializer();
		this.valueSerializer = (Serializer<V>)  context.valueSerde().serializer();
		this.keyDeserializer = (Deserializer<K>) context.keySerde().deserializer();
		this.valueDeserializer = (Deserializer<V>) context.valueSerde().deserializer();
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub	
	}

	@Override
	public void close() {
		if ( this.store != null ) this.store.close();
		if ( this.connection != null ) this.connection.close();
		this.open = false;
	}

	@Override
	public boolean persistent() {
		return true;
	}

	@Override
	public boolean isOpen() {
		return this.open;
	}

	@Override
	public V get(K key) {
		String id = this.keySerializer.serialize(this.topic, key).toString();
		byte[] data = this.store.findById(id).asJsonString().getBytes();
		
		return this.valueDeserializer.deserialize(this.topic, data);
	}

	@Override
	public KeyValueIterator<K, V> range(K from, K to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValueIterator<K, V> all() {
		// TODO Auto-generated method stub
		
		DocumentStream stream = this.store.find();
		
		
		return null;
	}

	@Override
	public long approximateNumEntries() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void put(K key, V value) {
		String id = this.keySerializer.serialize(this.topic,key).toString();
		Document doc = this.connection.newDocument(value);
		this.store.insertOrReplace(id,doc);	
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return null;
	}

	@Override
	public void putAll(List<KeyValue<K, V>> entries) {
		entries.forEach( (KeyValue<K, V> pair) -> put(pair.key, pair.value) );
	}

	@Override
	public V delete(K key) {
		String id = this.keySerializer.serialize(this.topic,key).toString();
		byte[] json = this.store.findById(id).asJsonString().getBytes();
		this.store.delete(id);
		
		return this.valueDeserializer.deserialize(this.topic, json);
	}


}
