package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class TableHeaderReader {

	private final CqlSessionProvider sessionProvider;
	private final QueryBuilder queryBuilder;

	public TableHeaderReader(CqlSessionProvider sessionProvider, QueryBuilder queryBuilder) {
		this.sessionProvider = sessionProvider;
		this.queryBuilder = queryBuilder;
	}


	public List<String> readAndSetHeaders() {
		String query = queryBuilder.getQuery();
		System.out.println("Using query: " + query);

		ResultSet rs = sessionProvider.getSession().execute(query);
		Row raw = rs.one();

		List<String> columns = new ArrayList<>();
		raw.getColumnDefinitions().forEach(d -> columns.add(d.getName().toString()));

		String list = StringUtils.join(columns.toArray(), ",");
		queryBuilder.setSelectFields(list);

		return columns;
	}
}
