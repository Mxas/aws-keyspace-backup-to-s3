package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

class DeleteRunable implements Runnable {

	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSession session;
	private final List<String> primaryKeys;
	private final ArrayBlockingQueue<Object[]> queue;
	private final LongAdder linesDeleted;

	DeleteRunable(KeyspaceQueryBuilder queryBuilder, CqlSession session, List<String> primaryKeys, ArrayBlockingQueue<Object[]> queue, LongAdder linesDeleted) {
		this.queryBuilder = queryBuilder;
		this.session = session;
		this.primaryKeys = primaryKeys;
		this.queue = queue;
		this.linesDeleted = linesDeleted;
	}

	@Override
	public void run() {

		PreparedStatement statment = prepareStatement(session);
		while (true) {
			try {
				BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
				buildBatch(statment, builder);

				if (builder.getStatementsCount() > 0) {
//				CompletionStage<AsyncResultSet> f = sessionProvider.getSession2().executeAsync(builder.build());
					//					pageLimiter.acquire();

					session.execute(builder.build());

//				f.whenComplete((rs, t) -> nexDelete(statment, latch));
				} else {
					System.out.println();
					System.out.println("no records " + Thread.currentThread().getName());
					System.out.println();
//				latch.countDown();
					break;
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
//			try {
//				latch.await();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}
		System.out.println("Finished " + Thread.currentThread().getName());
	}

//	private void nexDelete(PreparedStatement statment, CountDownLatch latch) {
//
//		BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
//		buildBatch(statment, builder);
//		if (builder.getStatementsCount() > 0) {
//			CompletionStage<AsyncResultSet> f = session.executeAsync(builder.build());
//			f.whenComplete((rs, t) -> nexDelete(statment, latch));
//		} else {
//			System.out.println("no records");
//			latch.countDown();
//		}
//	}

	private PreparedStatement prepareStatement(CqlSession session) {
		DeleteSelection d1 = QueryBuilder.deleteFrom(queryBuilder.getKeyspaceName(), queryBuilder.getTableName());
		List<Relation> wheres = new ArrayList<>();
		for (String primaryKey : this.primaryKeys) {
			wheres.add(Relation.column(primaryKey).isEqualTo(QueryBuilder.bindMarker()));
		}
		return session.prepare(d1.where(wheres).build());
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

	public Object[] poll() {
		try {
			return queue.poll(5, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

}
