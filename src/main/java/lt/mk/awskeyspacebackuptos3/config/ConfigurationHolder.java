package lt.mk.awskeyspacebackuptos3.config;

import org.apache.commons.lang3.StringUtils;

public class ConfigurationHolder {


	public AwsKeyspaceConf keyspace = new AwsKeyspaceConf();
	public Menu menu = new Menu();
	public Fs fs = new Fs();
	public S3 s3 = new S3();
	public InMemory memory = new InMemory();
	public Stat stat = new Stat();


	public class Menu {

		public String command;

		public String outputFilePath = "output_v1.txt";

		public boolean isMenuDisabled() {
			return StringUtils.isNotBlank(command);
		}
	}

	public class Stat {

		public long printStatisticInMillis = 2000;
		public long printHeaderAfterSeconds = 600;
		public long printStatNewLineAfterSeconds = 30;
		public int stopStatsPrintingAfterNotChangedSeconds = 1000;
	}

	public class Fs {

		public String storeTo;
	}

	public class AwsKeyspaceConf {

		public String awsKeyspaceDriverConfigPath;
		public String keyspace;
		public String table;
		public String query;

		public int pagesToSkip = 0;
		public int countOnEmptyPageReturnsFinish = 999999;
		public int reinsertTtl = 15552000;
		public int rateLimiterPerSec = 500;

		public int writeThreadsCount = 8;

		public int stopPageFetchingAfterErrorPage = 50;
		public int deleteBatchSize = 30;
		public long wantInQueueNewItemTimeoutMinutes = 10;
	}


	public class InMemory {

		public int queueSize = 500_000;
		public int singleStreamSizeInMB = 200;
		public long waitInQueueNewItemInSeconds = 180;
	}

	public class S3 {

		public String region;
		public String bucket;
		public String folder;

		public String storeFileNameSuffix;
		public String restoreFromCsv;
	}
}
