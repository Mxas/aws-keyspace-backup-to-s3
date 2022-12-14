package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class TestCountHelper {

	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;

	private final TableHeaderReader tableHeaderReader;
	private final LongAdder linesRead;
	private final AtomicInteger page;
	private final AtomicInteger emptyPagesCounter;
	private CountDownLatch latch;
	private Thread thread;

	public TestCountHelper(KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader) {
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.linesRead = new LongAdder();
		this.page = new AtomicInteger(0);
		this.emptyPagesCounter = new AtomicInteger(0);
	}

	public void startCounting() {
		if (isThreadActive()) {
			System.out.println("Already running");
		} else {
			init();
			startQuerying();
		}
	}

	private void init() {
		this.linesRead.reset();
		this.page.set(0);
		this.emptyPagesCounter.set(0);
		this.latch = new CountDownLatch(1);
	}

	private void startQuerying() {
		this.thread = ThreadUtil.newThreadStart(() -> {
			List<String> head = tableHeaderReader.readAndSetHeaders();
			CompletionStage<AsyncResultSet> futureRs = sessionProvider.getReadingSession().executeAsync(queryBuilder.getCountingQuery(head.get(0)));
			futureRs.whenComplete(this::processResultSetCounting);
			waitLatch();
			System.out.println("Counting finished");
		}, "query-t");
	}

	private void waitLatch() {
		ThreadUtil.await(latch,5, TimeUnit.DAYS);
	}

	void processResultSetCounting(AsyncResultSet rs, Throwable error) {
		KeyspaceUtil.checkError(error, page.incrementAndGet());
		int count = rs.remaining();
		linesRead.add(count);
		if (rs.hasMorePages()) {
			rs.fetchNextPage().whenComplete(this::processResultSetCounting);
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
}
