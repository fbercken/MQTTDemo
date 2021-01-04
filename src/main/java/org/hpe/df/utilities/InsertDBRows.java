package org.hpe.df.utilities;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;


public class InsertDBRows {
	

	public static void main(String[] args) {
		
		Document doc;
		DocumentStore store;
		ExecutorService executor = null;
		String storeName = "/tables/test";
		int rows = args.length > 0 ? Integer.valueOf(args[0]) : 10000;
		int threads = args.length > 1 ?  Integer.valueOf(args[1]) : 5;
		boolean buffered_writes =  args.length > 2 ? Boolean.valueOf(args[2]): false;
		
		System.out.format("Rows: %s - Threads: %s - Buffer: %s \n", rows, threads, buffered_writes);
		
		
		Connection connection = DriverManager.getConnection("ojai:mapr:");

		if ( buffered_writes) {
			doc = connection.newDocument().set("ojai.mapr.documentstore.buffer-writes", true);
			store = ( connection.storeExists(storeName) ) ? connection.getStore(storeName, doc) : connection.createStore(storeName, doc);
		} else {
			store = ( connection.storeExists(storeName) ) ? connection.getStore(storeName) : connection.createStore(storeName);
		}
	
		List<Future<Result>> futures;
		Collection<Callable<Result>> callables = new ArrayList<>();
	
		
		try {
			executor = Executors.newFixedThreadPool(threads);
			
			System.out.println("/* Load ------------------ */");
			callables.clear();
			IntStream.range(0,threads).forEach( i -> callables.add( createLoader( i, connection,store,rows)));
			futures = executor.invokeAll(callables);
			display(futures);
			
			System.out.println("/* Find ------------------ */");
			callables.clear();
			IntStream.range(0,threads).forEach( i -> callables.add( createFinder( i, connection,store,rows)));
			futures = executor.invokeAll(callables);
			display(futures);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}
		
	}
	
	
	public static void display(List<Future<Result>> futures) {
		

		final AtomicLong end = new AtomicLong();
		final AtomicLong start = new AtomicLong();
		final AtomicLong duration = new AtomicLong();
		final AtomicInteger ops = new AtomicInteger();
		
		futures.forEach( future -> {
			try {
				
				Result result = future.get();
				ops.addAndGet(result.rows);
				end.set( Math.max(end.get(), result.end));
				start.set( Math.max(start.get(), result.start));
				duration.addAndGet(result.duration);					
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		});
		
		System.out.format("Latency : %.2f ms -- Total: %.0f ops/sec\n",  (duration.get()/1E6/ops.get()), ops.get()*1E9/(end.get()-start.get()) );
	}
	
	
	public static Callable<Result> createLoader(int name, Connection connection, DocumentStore store, int count) {
		
		return new Callable<Result>() {

			Result result = new Result();
			
			@Override
			public Result call() throws Exception {
	
				result.start = System.nanoTime();
				
				IntStream.range(0, count).forEach( i -> {
					String key = String.format("%s:%s", name, i);
				
					long start = System.nanoTime();
					store.insertOrReplace( key, createDoc(key));
					result.addOperation(System.nanoTime() - start);

				});
				
				result.end = System.nanoTime();
				
				return result;
			}
			
			public Document createDoc(String uid) {
				return connection.newDocument()
					//.setId(uid )
					.set("field0","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field1","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field2","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field3","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field4","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field5","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field6","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field7","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field8","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")
					.set("field9","0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");
			}
			
		};
		
	}
	
	
	public static Callable<Result> createFinder(int name, Connection connection, DocumentStore store, int count) {
		
		return new Callable<Result>() {

			Result result = new Result();
			
			@Override
			public Result call()  {
				
				result.start = System.nanoTime();
				
				IntStream.range(0, count).forEach( i -> {
					String key = String.format("%s:%s", name, (int) Math.random()* count );			
					
					long start = System.nanoTime();
					store.findById(key);
					result.addOperation(System.nanoTime() - start);

				});
				
				result.end = System.nanoTime();
						
				return result;
			}
			
		};
	}

	
	static class Result {
		
		public int rows = 0;
		public long end;
		public long start;
		public long  duration = 0;  //nano seconds
		
		
		public void addOperation(long duration) {
			this.rows += 1;
			this.duration += duration;
		}
	}
	

}
