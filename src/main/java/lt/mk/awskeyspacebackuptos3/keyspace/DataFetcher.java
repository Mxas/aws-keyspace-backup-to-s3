package lt.mk.awskeyspacebackuptos3.keyspace;

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
import java.util.stream.Collectors;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

public class DataFetcher {

	public static final String DELIMITER = ",";
	private final AwsKeyspaceConf conf;
	private final QueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final DataQueue queue;
	private final LongAdder linesRead;
	private CountDownLatch latch;
	private AtomicInteger page;
	private Thread thread;

	public DataFetcher(AwsKeyspaceConf conf, QueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader, DataQueue queue) {
		this.conf = conf;
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.queue = queue;
		this.linesRead = new LongAdder();
		this.page = new AtomicInteger(0);
	}

	public void startReading() {

		if (isThreadActive()) {
			System.out.println("Already running");
		} else {

			init();

			List<String> head = tableHeaderReader.readAndSetHeaders();
			System.out.println("Found headers: " + head);
			String query = queryBuilder.getQueryForDataLoading();
			System.out.println("Using query: " + query);

			put(StringUtils.join(head.toArray(), DELIMITER));

			thread = new Thread(() -> {
				CompletionStage<AsyncResultSet> futureRs = sessionProvider.getSession().executeAsync(query);
				futureRs.whenComplete((rs, t) -> putInQueuePage(rs, t, head));

				waitLatch();
				System.out.println("Data fetching finished.");
			}, "KeyspaceDataFetcher");
			thread.start();
		}
	}

	private void init() {
		this.linesRead.reset();
		this.page.set(0);
		this.latch = new CountDownLatch(1);
	}

	private void putInQueuePage(AsyncResultSet rs, Throwable t, List<String> head) {
		page.incrementAndGet();
		try {
			for (Row row : rs.currentPage()) {
				String line = buildCsvLine(row, head);
				put(line);
			}
			if (rs.hasMorePages()) {
				rs.fetchNextPage().whenComplete((rs1, t1) -> putInQueuePage(rs1, t1, head));
			} else {
				latch.countDown();
			}
		} catch (Exception e) {
			e.printStackTrace();
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

	public int iterateAndCountAll() {
		async();
		return linesRead.intValue();
	}


	private void increment() {
		linesRead.increment();
//		if (linesRead.intValue() % 1000 == 0) {
//			System.out.println("Page " + page.intValue() + " found: " + linesRead.intValue());
//		}
	}

	private void async() {
		linesRead.reset();
		latch = new CountDownLatch(1);

		CompletionStage<AsyncResultSet> futureRs = sessionProvider.getSession().executeAsync(queryBuilder.getQuery());

		futureRs.whenComplete(this::processRowsAsync);

		waitLatch();
	}

	private void waitLatch() {
		try {
			latch.await(5, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	void processRowsAsync(AsyncResultSet rs, Throwable error) {
		int count = rs.remaining();
		linesRead.add(count);
		System.out.println("Count: " + linesRead.intValue());
		if (rs.hasMorePages()) {
			rs.fetchNextPage().whenComplete(this::processRowsAsync);
		} else {
			latch.countDown();
		}
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

	public void close(){
		try{
			if (isThreadActive()){
				thread.interrupt();
				thread.stop();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}