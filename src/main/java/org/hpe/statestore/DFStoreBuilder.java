package org.hpe.statestore;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.state.StoreBuilder;

public class DFStoreBuilder<K,V> implements StoreBuilder<DFStateStore<K,V>> {
	
	
	private final Logger log = LoggerFactory.getLogger(DFStoreBuilder.class);
	

	@Override
	public StoreBuilder<DFStateStore<K, V>> withCachingEnabled() {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public StoreBuilder<DFStateStore<K, V>> withLoggingEnabled(Map<String, String> config) {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public StoreBuilder<DFStateStore<K, V>> withLoggingDisabled() {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public DFStateStore<K, V> build() {
		// TODO Auto-generated method stub
		return new DFStateStore<K,V>();
	}

	@Override
	public Map<String, String> logConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean loggingEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return null;
	}

	//@Override
	public StoreBuilder<DFStateStore<K, V>> withCachingDisabled() {
		// TODO Auto-generated method stub
		return null;
	}

}
