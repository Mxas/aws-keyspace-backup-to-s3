package lt.mk.awskeyspacebackuptos3;

import java.util.Optional;
import lt.mk.awskeyspacebackuptos3.cli.CliCommandLine;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder;
import lt.mk.awskeyspacebackuptos3.fs.StoreToFile;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.inmemory.InputStreamProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.DataFetcher;
import lt.mk.awskeyspacebackuptos3.keyspace.QueryBuilder;
import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;
import lt.mk.awskeyspacebackuptos3.keyspace.TestCountHelper;
import lt.mk.awskeyspacebackuptos3.keyspace.TestQueryHelper;
import lt.mk.awskeyspacebackuptos3.s3.S3ClientWrapper;
import lt.mk.awskeyspacebackuptos3.s3.StoreToS3Service;
import lt.mk.awskeyspacebackuptos3.s3.StoreToS3TestService;
import lt.mk.awskeyspacebackuptos3.s3.SyncS3MultipartUploader;
import lt.mk.awskeyspacebackuptos3.statistic.StatisticPrinter;
import lt.mk.awskeyspacebackuptos3.statistic.StatisticProvider;

public class SingletonManager {

	private final ConfigurationHolder configurationHolder;
	private final CliCommandLine cliCommandLine;

	private CqlSessionProvider cqlSessionProvider;
	private TableHeaderReader tableHeaderReader;
	private QueryBuilder queryBuilder;
	private TestQueryHelper testQueryHelper;
	private TestCountHelper testCountHelper;
	private DataFetcher dataFetcher;
	private DataQueue queue;
	private StoreToFile storeToFile;
	private InputStreamProvider streamProvider;
	private S3ClientWrapper s3ClientWrapper;
	private SyncS3MultipartUploader syncS3MultipartUploader;
	private StoreToS3TestService storeToS3TestService;
	private StoreToS3Service storeToS3Service;
	private StatisticProvider statisticProvider;
	private StatisticPrinter statisticPrinter;

	public SingletonManager(String[] args) {
		this.configurationHolder = new ConfigurationHolder();
		this.cliCommandLine = new CliCommandLine(args, this.configurationHolder);

	}

	public void init() {
		try {
			this.cqlSessionProvider = new CqlSessionProvider(configurationHolder.keyspace);
			this.queryBuilder = new QueryBuilder(configurationHolder.keyspace);
			this.tableHeaderReader = new TableHeaderReader(cqlSessionProvider, queryBuilder);
			this.testQueryHelper = new TestQueryHelper(queryBuilder, cqlSessionProvider);
			this.testCountHelper = new TestCountHelper(queryBuilder, cqlSessionProvider, tableHeaderReader);
			this.queue = new DataQueue(configurationHolder.memory);
			this.streamProvider = new InputStreamProvider(queue, configurationHolder.memory);
			this.dataFetcher = new DataFetcher(configurationHolder.keyspace, queryBuilder, cqlSessionProvider, tableHeaderReader, queue);
			this.storeToFile = new StoreToFile(configurationHolder.fs, this.streamProvider);
			this.s3ClientWrapper = new S3ClientWrapper(configurationHolder.s3, configurationHolder.keyspace);
			this.syncS3MultipartUploader = new SyncS3MultipartUploader(this.s3ClientWrapper);
			this.storeToS3TestService = new StoreToS3TestService(s3ClientWrapper, this.syncS3MultipartUploader);
			this.storeToS3Service = new StoreToS3Service(this.s3ClientWrapper, this.streamProvider, this.syncS3MultipartUploader);
			this.statisticProvider = new StatisticProvider(queue, streamProvider, storeToFile, dataFetcher, storeToS3Service, s3ClientWrapper);
			this.statisticPrinter = new StatisticPrinter(statisticProvider);
		} catch (Exception e) {
			e.printStackTrace();
		}

//		this.statisticPrinter.iniStatPrinting();
	}

	public ConfigurationHolder getConfigurationHolder() {
		return configurationHolder;
	}

	public CliCommandLine getCliCommandLine() {
		return cliCommandLine;
	}

	public CqlSessionProvider getCqlSessionProvider() {
		return cqlSessionProvider;
	}

	public void close() {
		Optional.ofNullable(cqlSessionProvider).ifPresent(CqlSessionProvider::close);
		Optional.ofNullable(dataFetcher).ifPresent(DataFetcher::close);
		Optional.ofNullable(statisticPrinter).ifPresent(StatisticPrinter::close);
		Optional.ofNullable(testCountHelper).ifPresent(TestCountHelper::close);
	}

	public QueryBuilder getQueryBuilder() {
		return queryBuilder;
	}

	public TableHeaderReader getTableHeaderReader() {
		return tableHeaderReader;
	}

	public TestQueryHelper getTestQueryHelper() {
		return testQueryHelper;
	}

	public DataFetcher getDataFetcher() {
		return dataFetcher;
	}

	public DataQueue getQueue() {
		return queue;
	}

	public StoreToFile getStoreToFile() {
		return storeToFile;
	}

	public InputStreamProvider getStreamProvider() {
		return streamProvider;
	}

	public StoreToS3TestService getStoreToS3TestService() {
		return storeToS3TestService;
	}

	public S3ClientWrapper getS3ClientWrapper() {
		return s3ClientWrapper;

	}

	public SyncS3MultipartUploader getSyncS3MultipartUploader() {
		return syncS3MultipartUploader;
	}

	public StoreToS3Service getStoreToS3Service() {
		return storeToS3Service;
	}

	public StatisticPrinter getStatisticPrinter() {
		return statisticPrinter;
	}

	public TestCountHelper getTestCountHelper() {
		return testCountHelper;
	}
}
