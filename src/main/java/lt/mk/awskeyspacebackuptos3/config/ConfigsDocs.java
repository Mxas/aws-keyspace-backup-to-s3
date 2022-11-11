package lt.mk.awskeyspacebackuptos3.config;

import java.util.function.BiConsumer;
import java.util.function.Function;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder;

public enum ConfigsDocs {
	m("menu", "Use interactive menu.", (c, s) -> c.menu.disableMenu = !Boolean.parseBoolean(s), c -> String.valueOf(c.menu.disableMenu)),
	kk("keyspace-keyspace", "AWS Keyspace storage 'keyspace'. If query will be provided this value will be ignored.", (c, s) -> c.keyspace.keyspace = s,
			c -> c.keyspace.keyspace),
	kt("keyspace-table", "AWS Keyspace storage 'table'. If query will be provided this value will be ignored.", (c, s) -> c.keyspace.table = s,
			c -> c.keyspace.table),
	kq("keyspace-query", "AWS Keyspace data fetching query. Will ignoring keyspace.table if this value provided.", (c, s) -> c.keyspace.query = s,
			c -> c.keyspace.query),
	kf("keyspace-config-file", "AWS Keyspace configuration file path.", (c, s) -> c.keyspace.awsKeyspaceDriverConfigPath = s,
			c -> c.keyspace.awsKeyspaceDriverConfigPath),
	kp("keyspace-pages-to-skip", "AWS Keyspace pages to skip.", (c, s) -> c.keyspace.countOnEmptyPageReturnsFinish = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.countOnEmptyPageReturnsFinish)),
	ke("keyspace-emppty-to-finish", "AWS Keyspace returned empty pages assume as finished (max int).", (c, s) -> c.keyspace.countOnEmptyPageReturnsFinish = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.countOnEmptyPageReturnsFinish)),
	imq("in-memory-queue", "In memory blocking queue rows size.", (c, s) -> c.memory.queueSize = Integer.parseInt(s), c -> String.valueOf(c.memory.queueSize)),
	ims("in-memory-stream-size-mb", "In memory buffered stream size in MB.", (c, s) -> c.memory.singleStreamSizeInMB = Integer.parseInt(s), c -> String.valueOf(
			c.memory.singleStreamSizeInMB)),
	s3b("s3-bucket", "AWS S3 bucket.", (c, s) -> c.s3.bucket = s, c -> c.s3.bucket),
	s3f("s3-folder", "AWS S3 folder (object prefix in bucket).", (c, s) -> c.s3.folder = s, c -> c.s3.folder),
	s3r("s3-region", "AWS S3 bucket region.", (c, s) -> c.s3.region = s, c -> c.s3.region),
	fs("fs-result-path", "Path where to store files locally.", (c, s) -> c.fs.storeTo = s, c -> c.fs.storeTo);

	final String optionLong;
	final String description;
	final BiConsumer<ConfigurationHolder, String> set;
	final Function<ConfigurationHolder, String> get;

	ConfigsDocs(String optionLong, String description, BiConsumer<ConfigurationHolder, String> set, Function<ConfigurationHolder, String> get) {
		this.optionLong = optionLong;
		this.description = description;
		this.set = set;
		this.get = get;
	}

	public String getOptionLong() {
		return optionLong;
	}

	public String getDescription() {
		return description;
	}

	public BiConsumer<ConfigurationHolder, String> getSet() {
		return set;
	}

	public Function<ConfigurationHolder, String> getGet() {
		return get;
	}
}
