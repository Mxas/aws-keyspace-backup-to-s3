package lt.mk.awskeyspacebackuptos3.inmemory;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.InMemory;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class DataQueue {

	private final ArrayBlockingQueue<String> queue;

	public DataQueue(InMemory config) {
		queue = new ArrayBlockingQueue<>(config.queueSize);
	}

	public Optional<String> poll() {
		return ThreadUtil.wrap(() -> queue.poll(3, TimeUnit.MINUTES));
	}

	public void put(String line) {
		ThreadUtil.wrap(()->	queue.put(line));
	}

	public int size() {
		return queue.size();
	}
}
