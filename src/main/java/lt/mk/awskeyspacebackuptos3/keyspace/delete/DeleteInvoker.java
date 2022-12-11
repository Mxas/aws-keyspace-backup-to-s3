package lt.mk.awskeyspacebackuptos3.keyspace.delete;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceUtil;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.keyspace.TablePrimaryKeyReader;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;
import lt.mk.awskeyspacebackuptos3.statistic.Statistical;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class DeleteInvoker implements Statistical {

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
	private List<Thread> deletingThreads = new ArrayList<>();

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

			System.out.println("delete started");
		}
	}

	long getLinesDeleted() {
		return linesDeleted.longValue();
	}

	private void startLoadingQuery() {
		List<String> head = tableHeaderReader.readAndSetHeaders();
		System.out.println("Found headers: " + head);
		this.primaryKeys = tablePrimaryKeyReader.getPrimaryKeys();
		System.out.println("Primary keys: " + this.primaryKeys);
		String query = queryBuilder.getQueryForDataLoading();
		System.out.println("Build query: " + query);

		loadingQuery = ThreadUtil.newThread(() -> {
			CompletionStage<AsyncResultSet> futureRs = sessionProvider.getReadingSession().executeAsync(query);
			futureRs.whenComplete(this::putInQueuePage);
			waitLatch();
			sessionProvider.closeReadingSession();
			System.out.println("Data fetching finished.");
		}, "KeyspaceDataFetcher");

	}

	private void startDeleteQuery() {
		deletingThreads.clear();
		for (int i = 0; i < conf.writeThreadsCount; i++) {
			deletingThreads.add(ThreadUtil.newThreadStart(createDeleteRunnable(), "deleting-thread-" + i));

		}

		ThreadUtil.newThreadStart(() -> {
			while (getDeleteThreadsCount() > 0) {
				ThreadUtil.sleep3s();
			}
			sessionProvider.closeWriteSession();
		}, "reinsert-session-closeable");
	}

	public long getDeleteThreadsCount() {
		return deletingThreads.stream().filter(Thread::isAlive).count();
	}


	private Runnable createDeleteRunnable() {
		return new DeleteRunnable(queryBuilder, sessionProvider.getWriteSession(), primaryKeys, queue, linesDeleted, this.rateLimiter, conf.deleteBatchSize,
				conf.wantInQueueNewItemTimeoutMinutes, () -> loadingQuery != null && loadingQuery.isAlive() && latch != null && latch.getCount() > 0);
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

	private void putInQueuePage(AsyncResultSet rs, Throwable error) {
		if (State.isShutdown()) {
			System.out.println("System shutdown");
			return;
		}
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
				rs.fetchNextPage().whenComplete(this::putInQueuePage);
			} else {
				latch.countDown();
			}
		} catch (Exception e) {
			errorPagesCounter.incrementAndGet();
			e.printStackTrace();

			if (errorPagesCounter.intValue() < conf.stopPageFetchingAfterErrorPage && rs != null && rs.hasMorePages()) {
				sleepWhileQueueDecrease();
				rs.fetchNextPage().whenComplete(this::putInQueuePage);
			} else {
				System.out.println(String.format("After exception finishing errorPagesCounter:%s, rs: %s, hasMorePages: %s",
						errorPagesCounter.intValue(), rs != null, rs != null && rs.hasMorePages()));
				latch.countDown();
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
		ThreadUtil.wrap(() -> {
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
		ThreadUtil.await(latch, 5, TimeUnit.DAYS);
	}

	public boolean isThreadActive() {
		return ThreadUtil.isActive(loadingQuery);
	}

	public int getPage() {
		return page.get();
	}

	public int getQueueSize() {
		return queue.size();
	}

	public long getLinesRead() {
		return linesRead.longValue();
	}

	public void close() {
		ThreadUtil.stop(loadingQuery);
		deletingThreads.forEach(ThreadUtil::stop);
	}

	@Override
	public StatProvider provider() {
		return new DeleteStatistic(this);
	}
}
