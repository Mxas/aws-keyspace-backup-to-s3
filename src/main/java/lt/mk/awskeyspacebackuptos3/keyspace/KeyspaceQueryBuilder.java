package lt.mk.awskeyspacebackuptos3.keyspace;

import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import org.apache.commons.lang3.StringUtils;

public class KeyspaceQueryBuilder {

	private final AwsKeyspaceConf conf;
	private String selectFields;

	public KeyspaceQueryBuilder(AwsKeyspaceConf conf) {
		this.conf = conf;
	}

	public String getQueryForDataLoading() {
		if (StringUtils.isNotBlank(conf.query)) {
			return conf.query;
		}
		if (StringUtils.isNotBlank(selectFields)) {
			return appendFrom("select " + selectFields + " from ");
		}
		return appendFrom("select * from ");
	}

	public String getQuery() {
		if (StringUtils.isNotBlank(conf.query)) {
			return conf.query;
		}
		return appendFrom("select * from ");
	}

	public String getCountingQuery(String field) {
		if (StringUtils.isNotBlank(conf.query)) {
			return "select " + field + conf.query.substring(conf.query.toLowerCase().indexOf(" from "));
		}
		return appendFrom("select " + field + " from ");
	}

	public String getQueryJson() {
		if (StringUtils.isNotBlank(conf.query)) {
			return conf.query;
		}
		return appendFrom("select json * from ");
	}

	private String appendFrom(String select) {
		return select + conf.keyspace + "." + conf.table;
	}

	public void setSelectFields(String selectFields) {
		this.selectFields = selectFields;
	}

	public String getSelectFields() {
		return selectFields;
	}


	public String getKeyspaceName() {
		return conf.keyspace;
	}

	public String getTableName() {
		return conf.table;
	}
}
