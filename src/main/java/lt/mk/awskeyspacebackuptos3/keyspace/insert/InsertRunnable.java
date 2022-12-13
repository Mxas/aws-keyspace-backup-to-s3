package lt.mk.awskeyspacebackuptos3.keyspace.insert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;

class InsertRunnable implements Runnable {

	private final CqlSession session;
	private final List<String> header;
	private final DataQueue queue;
	private final LongAdder linesProcessed;
	private final LongAdder emptyCounter;
	private final String keyspaceName;
	private final String tableName;
	private final int ttl; //6days=518400
	private final RateLimiter rateLimiter;
	private final BooleanSupplier dataPopulationIsNotFinished;
	private final int batchSize;

	InsertRunnable(CqlSession session, List<String> header, DataQueue queue, LongAdder linesProcessed, String keyspaceName, String tableName,
			int ttl,
			RateLimiter rateLimiter, BooleanSupplier dataPopulationIsNotFinished, int batchSize) {
		this.session = session;
		this.header = new ArrayList<>(header);
		this.queue = queue;
		this.linesProcessed = linesProcessed;
		this.rateLimiter = rateLimiter;
		this.dataPopulationIsNotFinished = dataPopulationIsNotFinished;
		this.batchSize = batchSize;
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
					System.out.println(this.emptyCounter.intValue() + " no records to restore" + Thread.currentThread().getName());
					System.out.println();

					if (!this.dataPopulationIsNotFinished.getAsBoolean()) {
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
			Optional<String> line = queue.poll();
			if (line.isEmpty() && queue.isFinished()) {
				System.out.println("Queue is empty stopping...");
				break;
			}
			if (line.isPresent()) {
				lines.add(line.get());
				linesProcessed.increment();
			}
		}
		return lines;
	}


	public long getLinesProcessed() {
		return linesProcessed.longValue();
	}
}
