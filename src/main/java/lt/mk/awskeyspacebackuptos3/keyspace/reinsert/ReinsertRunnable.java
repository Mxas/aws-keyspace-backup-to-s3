package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import lt.mk.awskeyspacebackuptos3.State;

class ReinsertRunnable implements Runnable {

	public static final int WAITING_NEW_ITEM_TIMEOUT = 5;
	public static final int EMPTY_RESPONSE_COUNT = 10;
	private final CqlSession session;
	private final List<String> primaryKeys;
	private final List<String> header;
	private final ReQueue queue;
	private final LongAdder linesProcessed;
	private final LongAdder emptyCounter;
	private final String keyspaceName;
	private final String tableName;
	private final int ttl; //6days=518400

	private final RateLimiter rateLimiter;
	private Map<String, Term> headerMap;

	ReinsertRunnable(CqlSession session, List<String> primaryKeys, List<String> header, ReQueue queue, LongAdder linesProcessed, String keyspaceName, String tableName,
			int ttl,
			RateLimiter rateLimiter) {
		this.session = session;
		this.primaryKeys = new ArrayList<>(primaryKeys);
		this.header = new ArrayList<>(header);
		this.queue = queue;
		this.linesProcessed = linesProcessed;
		this.rateLimiter = rateLimiter;
		this.emptyCounter = new LongAdder();
		this.ttl = ttl;
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;
	}

	@Override
	public void run() {

		headerMap = getHeaderMap();

		PreparedStatement deleteStatement = prepareDeleteStatement(session);
		PreparedStatement insertStatement = prepareInsertStatement(session);
		while (State.isRunning()) {
			try {
				BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
				buildBatch(deleteStatement, insertStatement, builder);

				if (builder.getStatementsCount() > 0) {

					rateLimiter.acquire(builder.getStatementsCount());

					session.execute(builder.build());

				} else {
					this.emptyCounter.increment();
					System.out.println();
					System.out.println(this.emptyCounter.intValue() + "no records " + Thread.currentThread().getName());
					System.out.println();

					if (this.emptyCounter.intValue() > EMPTY_RESPONSE_COUNT) {
						break;
					}
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("Finished " + Thread.currentThread().getName());
	}


	private PreparedStatement prepareDeleteStatement(CqlSession session) {
		DeleteSelection d1 = QueryBuilder.deleteFrom(keyspaceName, tableName);
		List<Relation> wheres = new ArrayList<>();
		for (String primaryKey : this.primaryKeys) {
			wheres.add(Relation.column(primaryKey).isEqualTo(QueryBuilder.bindMarker()));
		}
		return session.prepare(d1.where(wheres).build());
	}

	private PreparedStatement prepareInsertStatement(CqlSession session) {

		RegularInsert insert = QueryBuilder
				.insertInto(keyspaceName, tableName)
				.values(headerMap);

		return session.prepare(insert.usingTtl(this.ttl).build());
	}

	private Map<String, Term> getHeaderMap() {
		Map<String, Term> values = new TreeMap<>();
		for (String name : this.header) {
			values.put(name, QueryBuilder.bindMarker());
		}
		return values;
	}

	private void buildBatch(PreparedStatement delete, PreparedStatement insert, BatchStatementBuilder builder) {
		for (int i = 0; i < 14; i++) {
			Optional<Object[]> arg = queue.poll();
			if (arg.isEmpty() && i == 0) {
				System.out.println("Finished delete");
				break;
			}
			if (arg.isPresent()) {
				builder.addStatement(delete.bind(deleteArgs(arg.get())));
				builder.addStatement(insert.bind(insertArgs(arg.get())));
				linesProcessed.increment();
			}
		}
	}

	private Object[] deleteArgs(Object[] arg) {
		Object[] del = new Object[primaryKeys.size()];
		int i = 0;
		for (String key : primaryKeys) {
			del[i] = arg[header.indexOf(key)];
			i++;
		}
		return del;
	}


	private Object[] insertArgs(Object[] arg) {
		Object[] del = new Object[header.size()];
		int i = 0;
		for (String key : this.headerMap.keySet()) {
			del[i] = arg[header.indexOf(key)];
			i++;
		}
		return del;
	}

	public long getLinesProcessed() {
		return linesProcessed.longValue();
	}
}
