package lt.mk.awskeyspacebackuptos3.inmemory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataQueue {


	public static final int CAPACITY = 500 * 1000;
	private final ArrayBlockingQueue<String> queue;

	public DataQueue() {
		queue = new ArrayBlockingQueue<>(CAPACITY);
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
