package lt.mk.awskeyspacebackuptos3.keyspace.insert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lt.mk.awskeyspacebackuptos3.csv.CsvLine;
import org.apache.commons.lang3.StringUtils;

public class InsertStatementPreparation {

	private final List<String> header;
	private final String keyspaceName;
	private final String tableName;
	private final int ttl;
	private final PreparedStatement insertStatement;
	private final Map<String, Term> headerMap;

	public InsertStatementPreparation(List<String> header, String keyspaceName, String tableName, int ttl, CqlSession session) {
		this.header = header;
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;
		this.ttl = ttl;
		this.headerMap = getHeaderMap();
		this.insertStatement = prepareInsertStatement(session);
	}

	private Map<String, Term> getHeaderMap() {
		Map<String, Term> values = new TreeMap<>();
		for (String name : this.header) {
			values.put(name, QueryBuilder.bindMarker());
		}
		return values;
	}

	private PreparedStatement prepareInsertStatement(CqlSession session) {

		RegularInsert insert = QueryBuilder
				.insertInto(keyspaceName, tableName)
				.values(headerMap);

		return session.prepare(insert.usingTtl(this.ttl).build());
	}

	public BatchStatement buildBatchStatement(List<String> lines) {
		BatchStatementBuilder builder = BatchStatement.builder(BatchType.UNLOGGED);
		for (List<String> args : CsvLine.csvLinesParse(lines)) {
			builder.addStatement(insertStatement.bind(insertArgs(args)));
		}
		return builder.build();
	}

	private Object[] insertArgs(List<String> args) {
		if (header.size() != args.size()) {
			throw new IllegalArgumentException("Line fields count do not match headers");
		}

		Object[] insertArgs = new Object[header.size()];
		int i = 0;
		for (String key : this.headerMap.keySet()) {
			insertArgs[i] = StringUtils.trimToNull(args.get(header.indexOf(key)));
			i++;
		}

		return insertArgs;
	}
}


