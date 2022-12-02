package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceUtil;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class LoadDataRunnable implements Runnable {

	private final AwsKeyspaceConf conf;
	private final CountDownLatch latch;
	private final List<String> header;
	private final CqlSession session;
	private final String query;
	private final AtomicInteger pageCounter;
	private final AtomicInteger errorPagesCounter;
	private final ReQueue queue;

	LoadDataRunnable(AwsKeyspaceConf conf, List<String> header, CqlSession session, String query, ReQueue queue) {
		this.conf = conf;
		this.header = header;
		this.session = session;
		this.query = query;
		this.queue = queue;
		this.latch = new CountDownLatch(1);
		this.pageCounter = new AtomicInteger();
		this.errorPagesCounter = new AtomicInteger();
	}

	@Override
	public void run() {
		CompletionStage<AsyncResultSet> futureRs = this.session.executeAsync(query);
		futureRs.whenComplete((rs, t) -> putInQueuePage(rs, t, header));
		waitLatch();
		System.out.println("Data fetching finished.");
	}

	private void putInQueuePage(AsyncResultSet rs, Throwable error, List<String> head) {

		try {
			KeyspaceUtil.checkError(error, pageCounter.get(), rs);
			pageCounter.incrementAndGet();
			if (conf.pagesToSkip < pageCounter.get()) {

				for (Row row : rs.currentPage()) {
					queue.put(args(row));
				}
			}
			if (rs.hasMorePages()) {

				queue.sleepWhileQueueDecrease();

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

	private void waitLatch() {
			ThreadUtil.await(latch,5, TimeUnit.DAYS);
	}

	private Object[] args(Row raw) {
		Object[] args = new Object[this.header.size()];
		ColumnDefinitions definitions = raw.getColumnDefinitions();

		for (int i = 0; i < definitions.size(); i++) {
			ColumnDefinition definition = definitions.get(i);
			String name = definition.getName().asCql(true);

			int index = this.header.indexOf(name);
			if (index > -1) {
				TypeCodec<Object> codec = raw.codecRegistry().codecFor(definition.getType());
				args[index] = codec.decode(raw.getBytesUnsafe(i), raw.protocolVersion());
			}
		}
		return args;
	}

	public int getPageCounter() {
		return pageCounter.get();
	}

	public int getErrorPagesCounter() {
		return errorPagesCounter.get();
	}
}
