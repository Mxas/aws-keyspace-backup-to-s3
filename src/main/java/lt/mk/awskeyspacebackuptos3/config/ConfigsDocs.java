package lt.mk.awskeyspacebackuptos3.config;

import java.util.function.BiConsumer;
import java.util.function.Function;

public enum ConfigsDocs {
	command("command", "Skip menu and execute [backup, restore, reinsert, delete] command. Otherwise use interactive menu.", (c, s) -> c.menu.command = s,
			c -> c.menu.command),
	kk("keyspace-keyspace", "AWS Keyspace storage 'keyspace'. If query will be provided this value will be ignored.", (c, s) -> c.keyspace.keyspace = s,
			c -> c.keyspace.keyspace),
	kt("keyspace-table", "AWS Keyspace storage 'table'. If query will be provided this value will be ignored.", (c, s) -> c.keyspace.table = s,
			c -> c.keyspace.table),
	kq("keyspace-query", "AWS Keyspace data fetching query. Will ignoring keyspace.table if this value provided.", (c, s) -> c.keyspace.query = s,
			c -> c.keyspace.query),
	kf("keyspace-config-file", "AWS Keyspace configuration file path.", (c, s) -> c.keyspace.awsKeyspaceDriverConfigPath = s,
			c -> c.keyspace.awsKeyspaceDriverConfigPath),
	kp("keyspace-pages-to-skip", "AWS Keyspace pages to skip.", (c, s) -> c.keyspace.pagesToSkip = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.pagesToSkip)),
	ke("keyspace-empty-to-finish", "AWS Keyspace returned empty pages assume as finished (max int).",
			(c, s) -> c.keyspace.countOnEmptyPageReturnsFinish = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.countOnEmptyPageReturnsFinish)),
	kttl("keyspace-reinsert-ttl-value", "AWS Keyspace reinsert ttl value (15552000 = 1y).", (c, s) -> c.keyspace.reinsertTtl = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.reinsertTtl)),
	krate("keyspace-update-rate-limiter-per-sec", "AWS Keyspace modify rate limiter (500!).", (c, s) -> c.keyspace.rateLimiterPerSec = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.rateLimiterPerSec)),
	kthrds("keyspace-write-thread-counts", "AWS Keyspace write (restore/reinsert/delete) threads count (default 8).",
			(c, s) -> c.keyspace.writeThreadsCount = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.writeThreadsCount)),
	kerrp("keyspace-stop-after-error-pages-count", "AWS Keyspace stop execution after errored/failed pages fetching.",
			(c, s) -> c.keyspace.stopPageFetchingAfterErrorPage = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.stopPageFetchingAfterErrorPage)),
	kdelb("keyspace-delete-batch-size", "AWS Keyspace delete batch size (aws max 30).",
			(c, s) -> c.keyspace.deleteBatchSize = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.deleteBatchSize)),
	kquewt("keyspace-wait-item-in-queue-mins", "AWS Keyspace operations wait records in queue time in minutes (15 min).",
			(c, s) -> c.keyspace.wantInQueueNewItemTimeoutMinutes = Integer.parseInt(s),
			c -> String.valueOf(c.keyspace.wantInQueueNewItemTimeoutMinutes)),
	imq("in-memory-queue", "In memory blocking queue rows size.", (c, s) -> c.memory.queueSize = Integer.parseInt(s), c -> String.valueOf(c.memory.queueSize)),
	ims("in-memory-stream-size-mb", "In memory buffered stream size in MB. If AWS s3 storage will be used, then this size will be one multi part size value in MB.",
			(c, s) -> c.memory.singleStreamSizeInMB = Integer.parseInt(s), c -> String.valueOf(
			c.memory.singleStreamSizeInMB)),
	s3b("s3-bucket", "AWS S3 bucket.", (c, s) -> c.s3.bucket = s, c -> c.s3.bucket),
	s3f("s3-folder", "AWS S3 folder (object prefix in bucket).", (c, s) -> c.s3.folder = s, c -> c.s3.folder),
	s3r("s3-region", "AWS S3 bucket region.", (c, s) -> c.s3.region = s, c -> c.s3.region),
	s3suf("s3-store-file-suffix", "AWS S3 file to store suffix (<timestamp>_<suffix>.csv).", (c, s) -> c.s3.storeFileNameSuffix = s, c -> c.s3.storeFileNameSuffix),
	s3res("s3-restore-from-csv-key", "AWS S3 file key to to restore from bucket (full path in bucket).", (c, s) -> c.s3.restoreFromCsv = s,
			c -> c.s3.restoreFromCsv),
	fs("fs-result-path", "Path where to store files locally.", (c, s) -> c.fs.storeTo = s, c -> c.fs.storeTo),
	stattime("stat-update-timeout-in-mills", "Statistic line refresh timeout in milliseconds.", (c, s) -> c.stat.printStatisticInMillis = Integer.parseInt(s),
			c -> String.valueOf(
					c.stat.printStatisticInMillis)),
	statline("stat-print-in-new-line-after-secs", "Statistic new line printing after seconds.", (c, s) -> c.stat.printStatNewLineAfterSeconds = Integer.parseInt(s),
			c -> String.valueOf(
					c.stat.printStatNewLineAfterSeconds)),
	statstop("stat-print-stop-after-no-changes-secs", "Statistic printing stopping after not changes found.",
			(c, s) -> c.stat.stopStatsPrintingAfterNotChangedSeconds = Integer.parseInt(s),
			c -> String.valueOf(
					c.stat.stopStatsPrintingAfterNotChangedSeconds)),
	statheadr("stat-reprint-header-after-seconds", "Statistic header reprinting after seconds.", (c, s) -> c.stat.printHeaderAfterSeconds = Integer.parseInt(s),
			c -> String.valueOf(
					c.stat.printHeaderAfterSeconds));

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
