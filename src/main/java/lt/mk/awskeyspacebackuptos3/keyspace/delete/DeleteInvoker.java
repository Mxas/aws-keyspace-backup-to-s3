package lt.mk.awskeyspacebackuptos3.keyspace.delete;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.keyspace.*;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

public class DeleteInvoker {

	private static final Logger LOG = Logger.getLogger(DeleteInvoker.class.getName());
	private final AwsKeyspaceConf conf;
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final TablePrimaryKeyReader tablePrimaryKeyReader;
	private final LongAdder linesRead;
	private final LongAdder linesDeleted;
	private CountDownLatch latch;
	private final AtomicInteger page;
	private final AtomicInteger emptyPagesCounter;
	private final AtomicInteger errorPagesCounter;
	private Thread loadingQuery;
	private List<String> primaryKeys;
	private final ArrayBlockingQueue<Object[]> queue;
	private RateLimiter rateLimiter;
	private Thread deletingQuery1;
	private Thread deletingQuery2;
	private Thread deletingQuery3;
	private Thread deletingQuery4;
	private Thread deletingQuery5;
	private long startSystemNanos;
	private int deletedLastCheckCount;
	private double lastRate;
	private Thread logThread;

	public DeleteInvoker(AwsKeyspaceConf conf, KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader,
			TablePrimaryKeyReader tablePrimaryKeyReader) {
		this.conf = conf;
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.tablePrimaryKeyReader = tablePrimaryKeyReader;
		this.linesRead = new LongAdder();
		this.linesDeleted = new LongAdder();
		this.page = new AtomicInteger(0);
		this.emptyPagesCounter = new AtomicInteger(0);
		this.errorPagesCounter = new AtomicInteger(0);
		this.queue = new ArrayBlockingQueue<>(100_000);
	}


	public void startR() {

		if (isThreadActive()) {
			System.out.println("Already running");
		} else {

			init();

			startLoadingQuery();
			startDeleteQuery();

			loadingQuery.start();
			deletingQuery1.start();
			deletingQuery2.start();
			deletingQuery3.start();
			deletingQuery4.start();
			deletingQuery5.start();

			initLogThread();
			System.out.println("delete started");
		}
	}

	private void initLogThread() {
		logThread = ThreadUtil.newThreadStart(() -> {
			while (State.isRunning()) {
				System.out.printf("\rQueue: %s, page: %s, linesRead: %s, linesDeleted: %s, rate: %.2f", queue.size(), getPage(), getLinesRead(),
						linesDeleted.intValue(), calcRate());
				try {
					if (linesDeleted.intValue() % 10 == 0) {
						System.out.print(" " + Arrays.toString(queue.peek()));
					}
					ThreadUtil.sleep1s();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		},"log-t");
	}

	private void startLoadingQuery() {
		List<String> head = tableHeaderReader.readAndSetHeaders();
		System.out.println("Found headers: " + head);
		this.primaryKeys = tablePrimaryKeyReader.getPrimaryKeys();
		System.out.println("Primary keys: " + this.primaryKeys);
		String query = queryBuilder.getQueryForDataLoading();
		System.out.println("Build query: " + query);

		loadingQuery = ThreadUtil.newThread(() -> {
			CompletionStage<AsyncResultSet> futureRs = sessionProvider.getSession().executeAsync(query);
			futureRs.whenComplete((rs, t) -> putInQueuePage(rs, t, head));
			waitLatch();
			System.out.println("Data fetching finished.");
		}, "KeyspaceDataFetcher");

	}

	private void startDeleteQuery() {
		deletingQuery1 = ThreadUtil.newThread(createRunnable(), "deletingQuery1");
		deletingQuery2 = ThreadUtil.newThread(createRunnable(), "deletingQuery2");
		deletingQuery3 = ThreadUtil.newThread(createRunnable(), "deletingQuery3");
		deletingQuery4 = ThreadUtil.newThread(createRunnable(), "deletingQuery4");
		deletingQuery5 = ThreadUtil.newThread(createRunnable(), "deletingQuery5");
	}


	private Runnable createRunnable() {
		return new DeleteRunnable(queryBuilder, sessionProvider.getSession2(), primaryKeys, queue, linesDeleted, this.rateLimiter);
	}


	private void init() {
		this.linesRead.reset();
		this.linesDeleted.reset();
		this.page.set(0);
		this.emptyPagesCounter.set(0);
		this.errorPagesCounter.set(0);
		this.latch = new CountDownLatch(1);

		this.rateLimiter = RateLimiter.create(conf.rateLimiterPerSec);
	}

	private void putInQueuePage(AsyncResultSet rs, Throwable error, List<String> head) {

		try {
			KeyspaceUtil.checkError(error, page.get(), rs);
			page.incrementAndGet();
			if (conf.pagesToSkip < page.get()) {
				if (rs.remaining() == 0) {
					emptyPagesCounter.incrementAndGet();
				} else {
					emptyPagesCounter.set(0);
				}
				for (Row row : rs.currentPage()) {
					put(args(row));
				}
			}
			if (emptyPagesCounter.intValue() < conf.countOnEmptyPageReturnsFinish && rs.hasMorePages()) {

				sleepWhileQueueDecrease();

				rs.fetchNextPage().whenComplete((rs1, t1) -> putInQueuePage(rs1, t1, head));
			} else {
				latch.countDown();
			}
		} catch (Exception e) {
			errorPagesCounter.incrementAndGet();
			e.printStackTrace();

			if (errorPagesCounter.intValue() < 50 && rs != null && rs.hasMorePages()) {
				rs.fetchNextPage().whenComplete((rs1, t1) -> putInQueuePage(rs1, t1, head));
			}
			throw e;
		}
	}

	private void sleepWhileQueueDecrease() {
		while (queue.remainingCapacity() < 20_000) {
			ThreadUtil.sleep3s();
		}
	}

	private void put(Object[] args) {
		ThreadUtil.wrap(()-> {
			queue.put(args);
			increment();
		});
	}

	private Object[] args(Row raw) {
		Object[] args = new Object[this.primaryKeys.size()];
		ColumnDefinitions definitions = raw.getColumnDefinitions();

		for (int i = 0; i < definitions.size(); i++) {
			ColumnDefinition definition = definitions.get(i);
			String name = definition.getName().asCql(true);

			int index = this.primaryKeys.indexOf(name);
			if (index > -1) {
				TypeCodec<Object> codec = raw.codecRegistry().codecFor(definition.getType());
				args[index] = codec.decode(raw.getBytesUnsafe(i), raw.protocolVersion());
			}
		}
		return args;
	}


	private void increment() {
		linesRead.increment();

	}


	private void waitLatch() {
			ThreadUtil.await(latch,5, TimeUnit.DAYS);
	}

	public boolean isThreadActive() {
		return loadingQuery != null && loadingQuery.isAlive();
	}

	public int getPage() {
		return page.get();
	}

	public long getLinesRead() {
		return linesRead.longValue();
	}

	public void close() {
		stop(loadingQuery);
		stop(deletingQuery1);
		stop(deletingQuery2);
		stop(deletingQuery3);
		stop(deletingQuery4);
		stop(deletingQuery5);
		stop(logThread);
	}

	private void stop(Thread thread) {
		try {
			if (thread != null) {
				thread.interrupt();
				thread.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public double calcRate() {
		double duration = (double) (System.nanoTime() - startSystemNanos) / 1_000_000_000L;
		if (duration < 5) {
			return lastRate;
		}
		startSystemNanos = System.nanoTime();
		long totalWriteOps = linesDeleted.intValue() - deletedLastCheckCount;
		deletedLastCheckCount = linesDeleted.intValue();
		double rate = (double) totalWriteOps / duration;
		lastRate = rate;
		return rate;
	}
}
