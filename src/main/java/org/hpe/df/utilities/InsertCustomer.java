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


public class InsertCustomer {
		

		public static void main(String[] args) {
			
			Document doc;
			DocumentStore store;
			ExecutorService executor = null;
			String storeName = "/metrics";
			int rows = args.length > 0 ? Integer.valueOf(args[0]) : 50;
			int threads = args.length > 1 ?  Integer.valueOf(args[1]) : 3;
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
						//.setId(uid)
						.set("name", "frederic")
						.set("firstname",  "berque")
						.set("address.city", "Rueil Malmaison")
						.set("address.zipcode", "92500");
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
						Document doc;
						String key = String.format("%s:%s", name, (int) Math.random()* count );			
						
						long start = System.nanoTime();
						doc = store.findById(key);
						result.addOperation(System.nanoTime() - start);
						//System.out.println( doc);

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
