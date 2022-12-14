package lt.mk.awskeyspacebackuptos3.keyspace.backup;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceUtil;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;
import lt.mk.awskeyspacebackuptos3.statistic.Statistical;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

public class DataFetcher implements Statistical {

	private static final Logger LOG = Logger.getLogger(DataFetcher.class.getName());
	public static final String DELIMITER = ",";
	private final AwsKeyspaceConf conf;
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final DataQueue queue;
	private final LongAdder linesRead;
	private CountDownLatch latch;
	private final AtomicInteger page;
	private final AtomicInteger emptyPagesCounter;
	private final AtomicInteger errorPagesCounter;
	private Thread thread;

	public DataFetcher(AwsKeyspaceConf conf, KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader,
			DataQueue queue) {
		this.conf = conf;
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.queue = queue;
		this.linesRead = new LongAdder();
		this.page = new AtomicInteger(0);
		this.emptyPagesCounter = new AtomicInteger(0);
		this.errorPagesCounter = new AtomicInteger(0);
	}

	public void startReading() {

		if (isThreadActive()) {
			System.out.println("Already running");
		} else {

			init();

			List<String> head = tableHeaderReader.readAndSetHeaders();
			System.out.println("Found headers: " + head);
			String query = queryBuilder.getQueryForDataLoading();
			System.out.println("Build query: " + query);

			put(StringUtils.join(head.toArray(), DELIMITER));

			thread = ThreadUtil.newThread(() -> {
				CompletionStage<AsyncResultSet> futureRs = sessionProvider.getReadingSession().executeAsync(query);
				futureRs.whenComplete((rs, t) -> putInQueuePage(rs, t, head));

				waitLatch();
				System.out.println("Data fetching finished.");
				sessionProvider.closeReadingSession();
				queue.dataLoadingFinished();
			}, "KeyspaceDataFetcher");
			thread.start();
		}
	}

	private void init() {
		this.linesRead.reset();
		this.page.set(0);
		this.emptyPagesCounter.set(0);
		this.errorPagesCounter.set(0);
		this.latch = new CountDownLatch(1);
	}

	private void putInQueuePage(AsyncResultSet rs, Throwable error, List<String> head) {
		if (State.isShutdown()) {
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
					String line = buildCsvLine(row, head);
					put(line);
				}
			}
			if (emptyPagesCounter.intValue() < conf.countOnEmptyPageReturnsFinish && rs.hasMorePages() && State.isRunning()) {
				rs.fetchNextPage().whenComplete((rs1, t1) -> putInQueuePage(rs1, t1, head));
			} else {
				latch.countDown();
			}
		} catch (Exception e) {
			errorPagesCounter.incrementAndGet();
			e.printStackTrace();

			if (errorPagesCounter.intValue() < 50 && rs != null && rs.hasMorePages()) {
				rs.fetchNextPage().whenComplete((rs1, t1) -> putInQueuePage(rs1, t1, head));
			} else {
				latch.countDown();
			}
			throw e;
		}
	}

	private void put(String line) {
		queue.put(line);
		increment();
	}

	private String buildCsvLine(Row raw, List<String> head) {
		return head.stream()
				.map(name -> readAsString(raw, name))
				.map(StringUtils::trimToEmpty)
				.map(StringEscapeUtils::escapeCsv)
				.collect(Collectors.joining(DELIMITER));
	}

	private String readAsString(Row raw, String name) {
		ColumnDefinition definition = raw.getColumnDefinitions().get(name);

		TypeCodec<Object> codec = raw.codecRegistry().codecFor(definition.getType());
		Object value = codec.decode(raw.getBytesUnsafe(name), raw.protocolVersion());

		return value == null ? null : value.toString();
	}


	private void increment() {
		linesRead.increment();
//		if (linesRead.intValue() % 1000 == 0) {
//			System.out.println("Page " + page.intValue() + " found: " + linesRead.intValue());
//		}
	}


	private void waitLatch() {
		ThreadUtil.wrap(() -> latch.await(5, TimeUnit.DAYS));
	}

	public boolean isThreadActive() {
		return thread != null && thread.isAlive();
	}

	public int getPage() {
		return page.get();
	}

	public long getLinesRead() {
		return linesRead.longValue();
	}

	public long getQueueSize() {
		return queue.size();
	}

	public void close() {
		try {
			if (isThreadActive()) {
				thread.interrupt();
				thread.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public StatProvider provider() {
		return new DataFetchingStat(this);
	}
}
