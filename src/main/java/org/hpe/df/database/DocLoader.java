package org.hpe.df.database;


import org.ojai.Document;
import java.io.Closeable;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DocLoader<V> implements Closeable {
	
	private static final Logger logger = LoggerFactory.getLogger(DocLoader.class);
	
	private final String storeName;
	private final DocumentStore store;
	private final Connection connection;
	private final Function<V,String> keyFunction;
	private final BiFunction<Connection,V,Document> documentFunction;
	
	
	private DocLoader(Builder<V> builder) {
		
		this.storeName = builder.storeName;
		this.keyFunction = builder.keyFunction;
		this.documentFunction = builder.documentFunction;
		
		this.connection = DriverManager.getConnection("ojai:mapr:");

		if ( connection.storeExists(storeName) ) {
			this.store = connection.getStore(this.storeName);
		} else {
			this.store = connection.createStore(this.storeName);
		}
	}
	
	
	public void insert(String key, V data) {		
		this.store.insertOrReplace( key, documentFunction.apply(connection,data));
	}
	
	
	public void insert(V data) {		
		this.store.insertOrReplace( keyFunction.apply(data), documentFunction.apply(connection,data));
	}

	
	public void close() {
		if ( store != null ) {
			store.close();
		}
		if ( connection != null ) {
			connection.close();
		}
	}
	
	
	
	
	public static class Builder<V> {
		
		private String storeName;
		private Function<V,String> keyFunction;
		private BiFunction<Connection,V,Document> documentFunction;
		
		
		public Builder<V> storeName(String storeName) {
			this.storeName = storeName;
			return this;
		}
		
		public Builder<V> keyFunction(Function<V,String> keyFunction) {
			this.keyFunction = keyFunction;
			return this;
		}
		
		public Builder<V> documentFunction(BiFunction<Connection,V,Document> documentFunction) {
			this.documentFunction = documentFunction;
			return this;
		}
		
		public DocLoader<V> build() {
			return new DocLoader<V>(this);
		}
		
	}
	

}
