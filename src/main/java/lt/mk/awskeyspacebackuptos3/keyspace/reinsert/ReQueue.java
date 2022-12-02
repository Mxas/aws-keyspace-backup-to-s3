package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

import java.util.Optional;
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
		ThreadUtil.wrap(()-> {
			queue.put(args);
			increment();
		} );
	}

	private void increment() {
		totalCounter.incrementAndGet();
	}


	public void sleepWhileQueueDecrease() {
		while (queue.remainingCapacity() < 20_000) {
			ThreadUtil.sleep3s();
		}
	}


	public Optional<Object[]> poll() {
			return ThreadUtil.wrap(()-> queue.poll(WAITING_NEW_ITEM_TIMEOUT, TimeUnit.MINUTES));
	}

	public int size() {
		return queue.size();
	}
}
