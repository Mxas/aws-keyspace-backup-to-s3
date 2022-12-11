package lt.mk.awskeyspacebackuptos3.keyspace;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TablePrimaryKeyReader {

	private final CqlSessionProvider sessionProvider;
	private final KeyspaceQueryBuilder queryBuilder;

	public TablePrimaryKeyReader(CqlSessionProvider sessionProvider, KeyspaceQueryBuilder queryBuilder) {
		this.sessionProvider = sessionProvider;
		this.queryBuilder = queryBuilder;
	}

	public List<String> getPrimaryKeys() {
		Optional<KeyspaceMetadata> ks = sessionProvider.getReadingSession().getMetadata().getKeyspace(queryBuilder.getKeyspaceName());
		if (ks.isPresent()) {
			Optional<TableMetadata> table = ks.get().getTable(queryBuilder.getTableName());
			if (table.isPresent()) {
				List<String> primaryKeys = table.get().getPrimaryKey().stream().map(c -> c.getName().asInternal()).collect(Collectors.toList());
				System.out.println(primaryKeys);
				return primaryKeys;
			}
		}
		throw new RuntimeException("Failed identify primary keys");
	}
}
