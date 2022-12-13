package lt.mk.awskeyspacebackuptos3.keyspace.insert;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.csv.CsvLine;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;
import lt.mk.awskeyspacebackuptos3.statistic.Statistical;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;
import org.apache.commons.lang3.StringUtils;

public class InsertInvoker implements Statistical {

	private final AwsKeyspaceConf conf;
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final DataQueue queue;

	private final LongAdder linesProcessed = new LongAdder();
	private final List<Thread> insertThread = new ArrayList<>();
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

			startInsertThreads();

			System.out.println("insert started");
		}
	}


	long getLinesInserted() {
		return linesProcessed.longValue();
	}

	int getQueueSize() {
		return queue.size();
	}

	private void init() {
		this.linesProcessed.reset();
		this.rateLimiter = RateLimiter.create(conf.rateLimiterPerSec, 10, TimeUnit.SECONDS);
	}

	private void startInsertThreads() {
		for (int i = 0; i < conf.writeThreadsCount; i++) {
			Thread t = ThreadUtil.newThread(createRunnable(), "insert-thread-" + i);
			insertThread.add(t);
			t.start();
		}

		ThreadUtil.newThreadStart(() -> {
			while (getInsertThreadsCount() > 0) {
				ThreadUtil.sleep3s();
			}
			sessionProvider.closeWriteSession();
		}, "insert-session-closeable");
	}

	private Runnable createRunnable() {
		BooleanSupplier dataPopulationIsNotFinished = () -> !queue.isFinished();
		return new InsertRunnable(sessionProvider.getWriteSession(), getHeaders(), queue, linesProcessed, queryBuilder.getKeyspaceName(), queryBuilder.getTableName(),
				conf.reinsertTtl, rateLimiter, dataPopulationIsNotFinished, conf.deleteBatchSize);
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

	public boolean isThreadActive() {
		return !insertThread.isEmpty() && insertThread.get(0).isAlive();
	}

	public void close() {

		insertThread.forEach(ThreadUtil::stop);
		insertThread.clear();
	}

	public long getInsertThreadsCount() {
		return insertThread.stream().filter(Thread::isAlive).count();
	}

	@Override
	public StatProvider provider() {
		return new InsertStatistic(this);
	}
}
