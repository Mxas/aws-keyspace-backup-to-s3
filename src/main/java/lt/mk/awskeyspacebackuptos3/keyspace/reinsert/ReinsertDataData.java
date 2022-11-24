package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.KeyspaceQueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.keyspace.TablePrimaryKeyReader;

public class ReinsertDataData {


	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;
	private final TableHeaderReader tableHeaderReader;
	private final TablePrimaryKeyReader tablePrimaryKeyReader;

	public ReinsertDataData(KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider, TableHeaderReader tableHeaderReader,
			TablePrimaryKeyReader tablePrimaryKeyReader) {
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
		this.tableHeaderReader = tableHeaderReader;
		this.tablePrimaryKeyReader = tablePrimaryKeyReader;
	}


	public void reinsert() {
//		InsertInto ins = QueryBuilder.insertInto(queryBuilder.getKeyspaceName(), queryBuilder.getTableName());

//		ins.value("name", "val").us


//		QueryBuilder.insertInto(queryBuilder.getKeyspaceName(), queryBuilder.getTableName()).va

	}

}
