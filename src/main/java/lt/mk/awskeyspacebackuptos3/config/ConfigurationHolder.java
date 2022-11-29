package lt.mk.awskeyspacebackuptos3.config;

public class ConfigurationHolder {


	public AwsKeyspaceConf keyspace = new AwsKeyspaceConf();
	public Menu menu = new Menu();
	public Csv csv = new Csv();
	public Fs fs = new Fs();
	public S3 s3 = new S3();
	public InMemory memory = new InMemory();


	public class Menu {

		public boolean disableMenu;
	}

	public class Csv {

		public String storeTo;
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
	}


	public class InMemory {

		public int queueSize = 500_000;
		public int singleStreamSizeInMB = 200;
	}

	public class S3 {

		public int partSizeInMB = 10;
		public String region;
		public String bucket;
		public String folder;
	}
}
