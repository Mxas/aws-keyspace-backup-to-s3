package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
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
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final TablePrimaryKeyReader tablePrimaryKeyReader;

	private Thread loadingQuery;
	private Thread reinserting;

	private final ReQueue queue;
	private List<String> header;
	private List<String> primaryKeys;
	private String query;
	private LoadDataRunnable loadingRUnnable;
	private final LongAdder linesReinserted = new LongAdder();
	private RateLimiter rateLimiter;


	public ReinsertDataInvoker(AwsKeyspaceConf conf, KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader,
			TablePrimaryKeyReader tablePrimaryKeyReader) {
		this.conf = conf;
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.tablePrimaryKeyReader = tablePrimaryKeyReader;
		this.queue = new ReQueue();
	}


	public void startR() {

		if (isThreadActive()) {
			System.out.println("Already running");
		} else {

			init();

			startLoadingQuery();
			startReinserting();

			System.out.println("delete started");
		}
	}

	long getReinsertedCount() {
		return linesReinserted.longValue();
	}

	int getErrorPagesCounter() {
		return loadingRUnnable==null?0:loadingRUnnable.getErrorPagesCounter();
	}

	int getPageCounter() {
		return loadingRUnnable==null?0:loadingRUnnable.getPageCounter();
	}

	int getQueueSize() {
		return queue.size();
	}


	private void startLoadingQuery() {

		loadingRUnnable = new LoadDataRunnable(conf, header, sessionProvider.getSession(), query, queue);
		loadingQuery = ThreadUtil.newThread(loadingRUnnable, "KeyspaceLoadData");

		startThread(loadingQuery);

	}

	private void startReinserting() {

		for (int i = 0; i < 6; i++) {
			startThread(createReinsertingThread());
		}
	}

	private void startThread(Thread thread) {
		thread.start();
	}

	private Thread createReinsertingThread() {
		return ThreadUtil.newThread(new ReinsertRunnable(sessionProvider.getSession2(), primaryKeys, header, queue, linesReinserted, queryBuilder.getKeyspaceName(),
				queryBuilder.getTableName(), conf.reinsertTtl, rateLimiter), "reinserting");
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
		ThreadUtil.stop(reinserting);
	}

	@Override
	public StatProvider provider() {
		return new ReinsertStatistic(this);
	}
}
