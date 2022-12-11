package lt.mk.awskeyspacebackuptos3.keyspace.delete;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

class DeleteRunnable implements Runnable {

	public static final int WAITING_NEW_ITEM_TIMEOUT = 15;
	public static final int EMPTY_RESPONSE_COUNT = 10;
	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSession session;
	private final List<String> primaryKeys;
	private final ArrayBlockingQueue<Object[]> queue;
	private final LongAdder linesDeleted;
	private final LongAdder emptyCounter;
	private final RateLimiter rateLimiter;
	private final int batchSize;
	private final long wantInQueueNewItemTimeoutMinutes;
	private final BooleanSupplier dataPopulationIsNotFinished;

	DeleteRunnable(KeyspaceQueryBuilder queryBuilder, CqlSession session, List<String> primaryKeys, ArrayBlockingQueue<Object[]> queue, LongAdder linesDeleted,
			RateLimiter rateLimiter, int batchSize, long wantInQueueNewItemTimeoutMinutes, BooleanSupplier dataPopulationIsNotFinished) {
		this.queryBuilder = queryBuilder;
		this.session = session;
		this.primaryKeys = primaryKeys;
		this.queue = queue;
		this.linesDeleted = linesDeleted;
		this.batchSize = batchSize;
		this.wantInQueueNewItemTimeoutMinutes = wantInQueueNewItemTimeoutMinutes;
		this.dataPopulationIsNotFinished = dataPopulationIsNotFinished;
		this.emptyCounter = new LongAdder();
		this.rateLimiter = rateLimiter;
	}

	@Override
	public void run() {

		PreparedStatement statment = prepareStatement(session);
		while (State.isRunning()) {
			try {
				BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
				buildBatch(statment, builder);

				if (builder.getStatementsCount() > 0) {
					rateLimiter.acquire(builder.getStatementsCount());
					session.execute(builder.build());
				} else {
					this.emptyCounter.increment();
					System.out.println();
					System.out.println(this.emptyCounter.intValue() + " no records " + Thread.currentThread().getName());
					System.out.println();

					if (!this.dataPopulationIsNotFinished.getAsBoolean()) {
						break;
					}
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
		System.out.println("Finished " + Thread.currentThread().getName());
	}

	private PreparedStatement prepareStatement(CqlSession session) {
		DeleteSelection d1 = QueryBuilder.deleteFrom(queryBuilder.getKeyspaceName(), queryBuilder.getTableName());
		List<Relation> wheres = new ArrayList<>();
		for (String primaryKey : this.primaryKeys) {
			wheres.add(Relation.column(primaryKey).isEqualTo(QueryBuilder.bindMarker()));
		}
		return session.prepare(d1.where(wheres).build());
	}

	private void buildBatch(PreparedStatement delete, BatchStatementBuilder builder) {
		for (int i = 0; i < this.batchSize; i++) {
			Optional<Object[]> arg = poll();
			if (arg.isPresent()) {
				builder.addStatement(delete.bind(arg.get()));
				linesDeleted.increment();
			} else {
				System.out.println("Empty queue ... ");
				break;
			}
		}
	}

	public Optional<Object[]> poll() {
		return ThreadUtil.wrap(() -> queue.poll(this.wantInQueueNewItemTimeoutMinutes, TimeUnit.MINUTES));
	}
}
