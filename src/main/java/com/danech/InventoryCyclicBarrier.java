package com.danech;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author Danech Kuntal
 *
 */
public final class InventoryCyclicBarrier {

	private static Logger LOGGER = Logger.getLogger(InventoryCyclicBarrier.class.getName());

	private CyclicBarrier barrier;

	// Multiple threads will use asynchronously.
	private final ConcurrentHashMap<String, Integer> map;

	private final List<Map<String, Integer>> skuQuantity;

	private final class ProcessInventory implements Runnable {
		private String endPoint;
		private Map<String, Integer> skus;

		public ProcessInventory(String endPoint, Map<String, Integer> sku) {
			this.endPoint = endPoint;
			this.skus = sku;
		}

		@Override
		public void run() {
			try {

				/**
				 * Actual server communication will be happened in the thread. Call server API
				 * and get the SKU and its quantity data here. For example below are the
				 */

				LOGGER.info(endPoint + " has beeb called and got data");

				/**
				 * Lets assume data has been stored in this.skus
				 */

				this.skus.forEach((k, v) -> {
					LOGGER.info(k + " " + v);
				});

				// Some computations
				this.skus.forEach((k, v) -> {
					map.computeIfPresent(k, (kPrev, vPrev) -> {
						return vPrev + v;
					});
					map.computeIfAbsent(k, (vN) -> v);
				});

				barrier.await();

			} catch (InterruptedException | BrokenBarrierException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
				
				// If any exception has been occurred we reset the barrier to its initial stage,
				// so it wont continue the thread execution|wait forever.
				// It wont execute the Barrier Runnable thread, the last step, which will be
				// used to store the data to database.
				barrier.reset();
			}
		}
	}

	public InventoryCyclicBarrier() {

		map = new ConcurrentHashMap<>();
		skuQuantity = new ArrayList<>();
		/**
		 * Here assume for understanding the static data we have initialized but in real
		 * project the ProcessInventory thread will call server API and get response;
		 */
		Map<String, Integer> serverResponse1 = new HashMap<>();
		serverResponse1.put("sku110", 1200);
		serverResponse1.put("sku111", 23);
		serverResponse1.put("sku120", 234);
		serverResponse1.put("sku113", 34);

		Map<String, Integer> serverResponse2 = new HashMap<>();
		serverResponse1.put("sku114", 121);
		serverResponse1.put("sku115", 43);
		serverResponse1.put("sku116", 23);
		serverResponse1.put("sku117", 45);

		Map<String, Integer> serverResponse3 = new HashMap<>();
		serverResponse1.put("sku114", 56);
		serverResponse1.put("sku119", 66);
		serverResponse1.put("sku120", 77);
		serverResponse1.put("sku113", 90);

		skuQuantity.add(serverResponse1);
		skuQuantity.add(serverResponse2);
		skuQuantity.add(serverResponse3);

		try {

			LOGGER.info("CyclicBarrier");
			int parties = skuQuantity.size();

			/**
			 * Barrier Runnable will be called at the last when ProcessInventory thread will
			 * be completed. This is for sum the all skus inventory and to store into the
			 * database.
			 */
			barrier = new CyclicBarrier(parties, () -> {

				LOGGER.info("Final skus and its quantity has been got, below are the details");
				map.forEach((k, v) -> {
					LOGGER.info(k + " " + v);
				});
				LOGGER.info("Service Layer has been called and data has been saved in the database");

			});
			for (int i = 0; i < parties; i++) {
				Thread t = new Thread(
						new ProcessInventory("http://dummy.endpoint/" + i + "/inventory", skuQuantity.get(i)));
				t.start();
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}
	public static void main(String[] args) {
		// Start application
		new InventoryCyclicBarrier();
	}
}