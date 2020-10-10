package org.hpe.statestore;

import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;


public class DFStateStore<K,V> implements StateStore  {
	
	private boolean open = false;
	private String storeName;
	private DocumentStore store;
	private Connection connection;

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init(ProcessorContext context, StateStore root) {
		
		this.storeName = (String) context.appConfigs().get("table");
		
		this.connection = DriverManager.getConnection("ojai:mapr:");		
		this.store = connection.storeExists(storeName)? connection.getStore(storeName) : connection.createStore(storeName);	
		this.open = true;
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



}
