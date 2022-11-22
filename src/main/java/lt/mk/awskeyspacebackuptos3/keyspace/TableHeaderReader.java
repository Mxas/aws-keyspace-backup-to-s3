package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class TableHeaderReader {

	private final CqlSessionProvider sessionProvider;
	private final KeyspaceQueryBuilder queryBuilder;

	public TableHeaderReader(CqlSessionProvider sessionProvider, KeyspaceQueryBuilder queryBuilder) {
		this.sessionProvider = sessionProvider;
		this.queryBuilder = queryBuilder;
	}


	public List<String> readAndSetHeaders() {
		String query = queryBuilder.getQuery();
		System.out.println("Using query: " + query);

		ResultSet rs = sessionProvider.getSession().execute(query);
		List<String> columns = new ArrayList<>();
		rs.getColumnDefinitions().forEach(d -> columns.add(d.getName().toString()));

		String list = StringUtils.join(columns.toArray(), ",");
		queryBuilder.setSelectFields(list);

		return columns;
	}
}
