package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
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
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;

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
	private final RateLimiter pageLimiter;
	private Thread deletingQuery1;
	private Thread deletingQuery2;
	private Thread deletingQuery3;
	private Thread deletingQuery4;
	private Thread deletingQuery5;
	private long startSystemNanos;
	private int deletedLastCheckCount;
	private double lastRate;
	private CqlSession session;

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
		this.pageLimiter = RateLimiter.create(20000);
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

			new Thread(() -> {
				while (true) {
					System.out.printf("\rQueue: %s, page: %s, linesRead: %s, linesDeleted: %s, rate: %.2f"
							, queue.size(), getPage(), getLinesRead(),
							linesDeleted.intValue(), calcRate());
					try {
						Thread.sleep(300L);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}).start();

			System.out.println("delete started");
		}
	}

	private void startLoadingQuery() {
		List<String> head = tableHeaderReader.readAndSetHeaders();
		System.out.println("Found headers: " + head);
		this.primaryKeys = tablePrimaryKeyReader.getPrimaryKeys();
		System.out.println("Primary keys: " + this.primaryKeys);
		String query = queryBuilder.getQueryForDataLoading();
		System.out.println("Build query: " + query);

		loadingQuery = new Thread(() -> {
			CompletionStage<AsyncResultSet> futureRs = sessionProvider.getSession().executeAsync(query);
			futureRs.whenComplete((rs, t) -> putInQueuePage(rs, t, head));
			waitLatch();
			System.out.println("Data fetching finished.");
		}, "KeyspaceDataFetcher");

	}

	private void startDeleteQuery() {

		Runnable runnable = () -> {
			CountDownLatch latch = new CountDownLatch(1);
			session = sessionProvider.createSession();
			PreparedStatement statment = prepareStatement(session);
			while (true) {
				BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
				buildBatch(statment, builder);

				if (builder.getStatementsCount() > 0) {
//				CompletionStage<AsyncResultSet> f = sessionProvider.getSession2().executeAsync(builder.build());
					//					pageLimiter.acquire();
					session.execute(builder.build());
//				f.whenComplete((rs, t) -> nexDelete(statment, latch));
				} else {
					System.out.println("no records");
//				latch.countDown();
					break;
				}

//			try {
//				latch.await();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			}
			System.out.println("Finished " + Thread.currentThread().getName());
		};
		deletingQuery1 = new Thread(runnable, "deletingQuery1");
		deletingQuery2 = new Thread(runnable, "deletingQuery2");
		deletingQuery3 = new Thread(runnable, "deletingQuery3");
		deletingQuery4 = new Thread(runnable, "deletingQuery4");
		deletingQuery5 = new Thread(runnable, "deletingQuery5");
	}

	private void nexDelete(PreparedStatement statment, CountDownLatch latch) {

		BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
		buildBatch(statment, builder);
		if (builder.getStatementsCount() > 0) {
			CompletionStage<AsyncResultSet> f = sessionProvider.getSession().executeAsync(builder.build());
			f.whenComplete((rs, t) -> nexDelete(statment, latch));
		} else {
			System.out.println("no records");
			latch.countDown();
		}
	}

	private void buildBatch(PreparedStatement delete, BatchStatementBuilder builder) {
		for (int i = 0; i < 30; i++) {
			Object[] arg = poll();
			if (arg == null && i == 0) {
				System.out.println("Finished delete");
				break;
			}
			if (arg != null) {
				builder.addStatement(delete.bind(arg));
				linesDeleted.increment();
			}
		}
	}

	private PreparedStatement prepareStatement(CqlSession session) {
		DeleteSelection d1 = QueryBuilder.deleteFrom(queryBuilder.getKeyspaceName(), queryBuilder.getTableName());
		List<Relation> wheres = new ArrayList<>();
		for (String primaryKey : this.primaryKeys) {
			wheres.add(Relation.column(primaryKey).isEqualTo(QueryBuilder.bindMarker()));
		}
		return session.prepare(d1.where(wheres).build());
	}

	private void init() {
		this.linesRead.reset();
		this.linesDeleted.reset();
		this.page.set(0);
		this.emptyPagesCounter.set(0);
		this.errorPagesCounter.set(0);
		this.latch = new CountDownLatch(1);
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
			try {
				Thread.sleep(3000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void put(Object[] args) {
		try {
			queue.put(args);
			increment();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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

	public Object[] poll() {
		try {
			return queue.poll(5, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void waitLatch() {
		try {
			latch.await(5, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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
		try {
			if (isThreadActive()) {
				loadingQuery.interrupt();
				loadingQuery.stop();
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
		if (rate > 0) {
			lastRate = rate;
		}
		return rate;
	}
}
