package lt.mk.awskeyspacebackuptos3.inmemory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.InMemory;

public class DataQueue {

	private final ArrayBlockingQueue<String> queue;

	public DataQueue(InMemory config) {
		queue = new ArrayBlockingQueue<>(config.queueSize);
	}

	public String poll() {
		try {
			return queue.poll(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void put(String line) {
		try {
			queue.put(line);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public int size() {
		return queue.size();
	}
}
