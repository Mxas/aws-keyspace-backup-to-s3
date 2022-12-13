package lt.mk.awskeyspacebackuptos3.inmemory;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.InMemory;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class DataQueue {

	private final ArrayBlockingQueue<String> queue;
	private final InMemory config;
	private boolean finished = false;

	public DataQueue(InMemory config) {
		this.config = config;
		queue = new ArrayBlockingQueue<>(this.config.queueSize);
	}

	public Optional<String> poll() {
		return ThreadUtil.wrap(() -> queue.poll(config.waitInQueueNewItemInSeconds, TimeUnit.SECONDS));
	}

	public void put(String line) {
		ThreadUtil.wrap(() -> queue.put(line));
	}

	public int size() {
		return queue.size();
	}

	public void dataLoadingFinished() {
		this.finished = true;
	}

	public boolean isFinished() {
		return finished && queue.size() == 0;
	}
}
