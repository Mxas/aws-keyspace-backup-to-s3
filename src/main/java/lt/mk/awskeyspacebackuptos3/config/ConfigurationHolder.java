package lt.mk.awskeyspacebackuptos3.config;

public class ConfigurationHolder {


	public AwsKeyspaceConf keyspace = new AwsKeyspaceConf();
	public Menu menu = new Menu();
	public Csv csv = new Csv();
	public Fs fs = new Fs();
	public S3 s3 = new S3();


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
	}

	public class S3 {

		public int partSizeInMB = 10;
		public String region;
		public String bucket;
		public String folder;
	}
}
