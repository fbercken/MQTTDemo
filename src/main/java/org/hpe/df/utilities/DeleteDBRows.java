package org.hpe.df.utilities;


import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;


public class DeleteDBRows {

	public static void main(String[] args) {
		
		String storeName = "/metrics";
		DocumentStore store;
		Connection connection = DriverManager.getConnection("ojai:mapr:");

		if ( connection.storeExists(storeName) ) {
			store = connection.getStore(storeName);
		} else {
			store = connection.createStore(storeName);
		}

		
		Query query = connection.newQuery().build();
		
		store.find(query).forEach( doc -> store.delete(doc) );
	}

}
