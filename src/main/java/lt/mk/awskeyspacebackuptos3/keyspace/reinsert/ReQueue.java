package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import static lt.mk.awskeyspacebackuptos3.thread.ThreadUtil.wrap;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class ReQueue {

	private final ArrayBlockingQueue<Object[]> queue;
	private final AtomicInteger totalCounter;


	public ReQueue() {
		queue = new ArrayBlockingQueue<>(50_000);
		totalCounter = new AtomicInteger();
	}


	public void put(Object[] args) {
		wrap(() -> {
			queue.put(args);
			increment();
		});
	}

	private void increment() {
		totalCounter.incrementAndGet();
	}


	public void sleepWhileQueueDecrease() {
		while (queue.remainingCapacity() < 20_000) {
			ThreadUtil.sleep3s();
		}
	}


	public Optional<Object[]> poll(long waitingNewItemTimeoutInMinutes) {
		return wrap(() -> queue.poll(waitingNewItemTimeoutInMinutes, TimeUnit.MINUTES));
	}

	public int size() {
		return queue.size();
	}
}
