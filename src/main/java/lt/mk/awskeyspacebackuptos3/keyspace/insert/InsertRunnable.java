package lt.mk.awskeyspacebackuptos3.keyspace.insert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;

class InsertRunnable implements Runnable {

	public static final int EMPTY_RESPONSE_COUNT = 10;
	private final CqlSession session;
	private final List<String> header;
	private final DataQueue queue;
	private final LongAdder linesProcessed;
	private final LongAdder emptyCounter;
	private final String keyspaceName;
	private final String tableName;
	private final int ttl; //6days=518400

	private final RateLimiter rateLimiter;
	private int batchSize = 30;

	InsertRunnable(CqlSession session, List<String> header, DataQueue queue, LongAdder linesProcessed, String keyspaceName, String tableName,
			int ttl,
			RateLimiter rateLimiter) {
		this.session = session;
		this.header = new ArrayList<>(header);
		this.queue = queue;
		this.linesProcessed = linesProcessed;
		this.rateLimiter = rateLimiter;
		this.emptyCounter = new LongAdder();
		this.ttl = ttl;
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;
	}

	@Override
	public void run() {

		InsertStatementPreparation helper = new InsertStatementPreparation(header, keyspaceName, tableName, ttl, session);

		while (State.isRunning()) {
			try {

				List<String> lines = readLines();

				if (lines.size() > 0) {
					rateLimiter.acquire(lines.size());
					session.execute(helper.buildBatchStatement(lines));
				} else {
					this.emptyCounter.increment();
					System.out.println();
					System.out.println(this.emptyCounter.intValue() + "no records " + Thread.currentThread().getName());
					System.out.println();

					if (this.emptyCounter.intValue() > EMPTY_RESPONSE_COUNT) {
						break;
					}
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("Finished " + Thread.currentThread().getName());
	}

	private List<String> readLines() {
		List<String> lines = new ArrayList<>();
		for (int i = 0; i < batchSize; i++) {
			String line = queue.poll();
			if (line == null && i == 0) {
				System.out.println("Queue is empty stopping...");
				break;
			}
			if (line != null) {
				lines.add(line);
				linesProcessed.increment();
			}
		}
		return lines;
	}


	public long getLinesProcessed() {
		return linesProcessed.longValue();
	}
}
