package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.InMemory;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.keyspace.TablePrimaryKeyReader;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;
import lt.mk.awskeyspacebackuptos3.statistic.Statistical;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class ReinsertDataInvoker implements Statistical {

	private static final Logger LOG = Logger.getLogger(ReinsertDataInvoker.class.getName());
	private final AwsKeyspaceConf conf;

	private final InMemory inMemoryConf;
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final TablePrimaryKeyReader tablePrimaryKeyReader;

	private Thread loadingQuery;
	private final List<Thread> reinsertingThreads = new ArrayList<>();

	private final ReQueue queue;
	private List<String> header;
	private List<String> primaryKeys;
	private String query;
	private LoadDataRunnable loadingRunnable;
	private final LongAdder linesReinserted = new LongAdder();
	private RateLimiter rateLimiter;


	public ReinsertDataInvoker(AwsKeyspaceConf conf, InMemory inMemoryConf, KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader,
			TablePrimaryKeyReader tablePrimaryKeyReader) {
		this.conf = conf;
		this.inMemoryConf = inMemoryConf;
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.tablePrimaryKeyReader = tablePrimaryKeyReader;
		this.queue = new ReQueue();
	}


	public void startReinserting() {

		if (isThreadActive()) {
			System.out.println("Already running");
		} else {

			init();

			startLoadingQuery();
			startReinsertingThreads();

			System.out.println("Reinsert invoked");
		}
	}

	long getReinsertedCount() {
		return linesReinserted.longValue();
	}

	int getErrorPagesCounter() {
		return loadingRunnable == null ? 0 : loadingRunnable.getErrorPagesCounter();
	}

	int getPageCounter() {
		return loadingRunnable == null ? 0 : loadingRunnable.getPageCounter();
	}

	int getQueueSize() {
		return queue.size();
	}


	private void startLoadingQuery() {
		loadingRunnable = new LoadDataRunnable(conf, header, sessionProvider.getReadingSession(), query, queue);
		loadingQuery = ThreadUtil.newThreadStart(loadingRunnable, "KeyspaceLoadData");
	}

	private void startReinsertingThreads() {

		for (int i = 0; i < conf.writeThreadsCount; i++) {
			reinsertingThreads.add(createReinsertingThread(i));
		}

		ThreadUtil.newThreadStart(() -> {
			while (getReinsertThreadsCount() > 0) {
				ThreadUtil.sleep3s();
			}
			sessionProvider.closeWriteSession();
		}, "reinsert-session-closeable");
	}

	private Thread createReinsertingThread(int i) {
		BooleanSupplier dataPopulationIsNotFinished = () -> loadingQuery != null && loadingQuery.isAlive();
		return ThreadUtil.newThreadStart(
				new ReinsertRunnable(sessionProvider.getWriteSession(), primaryKeys, header, queue, linesReinserted, queryBuilder.getKeyspaceName(),
						queryBuilder.getTableName(), conf.reinsertTtl, rateLimiter, inMemoryConf.waitInQueueNewItemInSeconds, dataPopulationIsNotFinished),
				"reinserting-thread-" + i);
	}

	private void init() {

		header = tableHeaderReader.readAndSetHeaders();
		System.out.println("Found headers: " + header);
		query = queryBuilder.getQueryForDataLoading();
		System.out.println("Build query: " + query);
		this.primaryKeys = tablePrimaryKeyReader.getPrimaryKeys();
		System.out.println("Primary keys: " + this.primaryKeys);

		linesReinserted.reset();

		rateLimiter = RateLimiter.create(conf.rateLimiterPerSec);
	}


	public boolean isThreadActive() {
		return loadingQuery != null && loadingQuery.isAlive();
	}


	public void close() {
		ThreadUtil.stop(loadingQuery);
		reinsertingThreads.forEach(ThreadUtil::stop);
	}

	@Override
	public StatProvider provider() {
		return new ReinsertStatistic(this);
	}

	public long getReinsertThreadsCount() {
		return reinsertingThreads.stream().filter(Thread::isAlive).count();
	}
}
