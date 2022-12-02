package lt.mk.awskeyspacebackuptos3.keyspace.insert;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.csv.CsvLine;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;
import org.apache.commons.lang3.StringUtils;

public class InsertInvoker {

	private final AwsKeyspaceConf conf;
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final DataQueue queue;

	private LongAdder linesProcessed;
	private final List<Thread> insertThread = new ArrayList<>();
	private double lastRate;
	private long startSystemNanos;
	private long deletedLastCheckCount;
	private RateLimiter rateLimiter;
	private String firstLine;

	public InsertInvoker(AwsKeyspaceConf conf, KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader,
			DataQueue queue) {
		this.conf = conf;
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.queue = queue;
	}


	public void startR() {

		if (isThreadActive()) {
			System.out.println("Already running");
		} else {

			init();

			startDeleteQuery();

			initLogThread();
			System.out.println("insert started");
		}
	}

	private void initLogThread() {
		 ThreadUtil.newThreadStart(() -> {
			while (State.isRunning()) {
				System.out.printf("\rQueue: %s, linseInserted: %s, rate: %.2f", queue.size(), linesProcessed.intValue(), calcRate());
					ThreadUtil.sleep1s();
			}
		},"log-");
	}

	private void init() {
		this.linesProcessed = new LongAdder();
		this.rateLimiter = RateLimiter.create(conf.rateLimiterPerSec, 10, TimeUnit.SECONDS);
	}

	private void startDeleteQuery() {
		for (int i = 0; i < conf.insertThreadCount; i++) {
			Thread t = ThreadUtil.newThread(createRunnable(), "insert-thread-" + i);
			insertThread.add(t);
			t.start();
		}
	}

	private Runnable createRunnable() {
		return new InsertRunnable(sessionProvider.getSession2(), getHeaders(), queue, linesProcessed, queryBuilder.getKeyspaceName(), queryBuilder.getTableName(),
				conf.reinsertTtl, rateLimiter);
	}

	private List<String> getHeaders() {
		if (StringUtils.isBlank(firstLine)) {
			Optional<String> line = queue.poll();
			if (line.isEmpty()) {
				throw new IllegalArgumentException("Headers are missing...");
			}
			this.firstLine = line.get();
		}

		List<String> headerList = CsvLine.csvLineParse(this.firstLine);
		System.out.println("Using Header: " + headerList);
		return headerList;
	}


	public double calcRate() {
		double duration = (double) (System.nanoTime() - startSystemNanos) / 1_000_000_000L;
		if (duration < 5) {
			return lastRate;
		}
		startSystemNanos = System.nanoTime();
		long totalWriteOps = linesProcessed.intValue() - deletedLastCheckCount;
		deletedLastCheckCount = linesProcessed.intValue();
		double rate = (double) totalWriteOps / duration;
		lastRate = rate;
		return rate;
	}


	public boolean isThreadActive() {
		return !insertThread.isEmpty() && insertThread.get(0).isAlive();
	}

	public void close() {

		insertThread.forEach(c -> c.stop());
		insertThread.clear();
	}
}
