package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.cql.ResultSet;

public class TestQueryHelper {

	private final KeyspaceQueryBuilder queryBuilder;
	private final CqlSessionProvider sessionProvider;

	public TestQueryHelper(KeyspaceQueryBuilder queryBuilder, CqlSessionProvider sessionProvider) {
		this.queryBuilder = queryBuilder;
		this.sessionProvider = sessionProvider;
	}

	public String testOneJson() {
		//select json * from " + conf.table + "  where timestamp > '2020-09-15 20:05:59.745Z' ALLOW FILTERING
		String query = queryBuilder.getQueryJson();
		System.out.println("Query: " + query);
		ResultSet rs = sessionProvider.getSession().execute(query);
		String json = rs.one().getString(0);
		return json;
	}

}
