package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ReQueue {

	private static final long WAITING_NEW_ITEM_TIMEOUT = 15;
	private final ArrayBlockingQueue<Object[]> queue;
	private final AtomicInteger totalCounter;


	public ReQueue() {
		queue = new ArrayBlockingQueue<>(50_000);
		totalCounter = new AtomicInteger();
	}


	public void put(Object[] args) {
		try {
			queue.put(args);
			increment();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void increment() {
		totalCounter.incrementAndGet();
	}


	public void sleepWhileQueueDecrease() {
		while (queue.remainingCapacity() < 20_000) {
			try {
				Thread.sleep(3000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	public Object[] poll() {
		try {
			return queue.poll(WAITING_NEW_ITEM_TIMEOUT, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int size() {
		return queue.size();
	}
}
