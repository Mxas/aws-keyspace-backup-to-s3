package lt.mk.awskeyspacebackuptos3.statistic;

import lt.mk.awskeyspacebackuptos3.fs.StoreToFile;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.inmemory.InputStreamProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.backup.DataFetcher;
import lt.mk.awskeyspacebackuptos3.s3.S3ClientWrapper;
import lt.mk.awskeyspacebackuptos3.s3.storing.StoreToS3Service;

@Deprecated // remove
public class StatisticProvider {

	private final DataQueue queue;
	private final InputStreamProvider streamProvider;
	private final StoreToFile storeToFile;
	private final DataFetcher dataFetcher;
	private final StoreToS3Service storeToS3Service;
	private final S3ClientWrapper s3ClientWrapper;

	private long startSystemNanos = System.nanoTime();
	private long readCount = 0;
	private double lastRate;

	public StatisticProvider(DataQueue queue, InputStreamProvider streamProvider, StoreToFile storeToFile, DataFetcher dataFetcher, StoreToS3Service storeToS3Service,
			S3ClientWrapper s3ClientWrapper) {
		this.queue = queue;
		this.streamProvider = streamProvider;
		this.storeToFile = storeToFile;
		this.dataFetcher = dataFetcher;
		this.storeToS3Service = storeToS3Service;
		this.s3ClientWrapper = s3ClientWrapper;
	}


	public String headerLine1() {
		if (fsNotActive()) {
			return String.format("|%42s|%37s|%44s|",
					"Data Fetching               ", "Memory Queue              ", "Aws S3 Storing              ");
		}
		return String.format("|%42s|%37s|%44s|%19s|",
				"Data Fetching               ", "Memory Queue              ", "Aws S3 Storing              ", "Storing To File  ");
	}

	private boolean fsNotActive() {
		return !storeToFile.isThreadActive() && storeToFile.getConsumedStreamCount() < 1;
	}


	public String headerLine2() {
		return dataFetcherH() + memoryQueueH() + s3H() + fsH() + "|";
	}

	public String dataLine() {
		return dataFetcher() + memoryQueue() + s3() + fs() + "|";
	}

	private String dataFetcherH() {
		return String.format("|%6s|%9s|%12s|%12s",
				"Active", "Page No ", "Total Lines ", "Rate p/s"
		);
	}

	private String dataFetcher() {
		return String.format("\u001b[32m|%6s|%9d|%12d|%12.2f\u001b[39m",
				dataFetcher.isThreadActive(), dataFetcher.getPage(), dataFetcher.getLinesRead(), calcRate()
		);
	}

	private String memoryQueueH() {
		return String.format("|%6s|%6s|%11s|%11s",
				"Queue", "Stream", "Lines Count", "Size B"
		);
	}

	private String memoryQueue() {
		return String.format("\u001b[33m|%6d|%6d|%11d|%11s\u001b[39m",
				queue.size(), streamProvider.getStreamCount(), streamProvider.getLinceCount(), StringFormatter.byteSize(streamProvider.getBytesCount())
		);
	}

	private String s3H() {
		return String.format("|%6s|%8s|%9s|%10s|%7s",
				"Active", "Consumed", "Last", "Total", "Part No"
		);
	}

	private String s3() {
		return String.format("\u001b[31m|%6s|%8d|%9s|%10s|%7d\u001b[39m",
				storeToS3Service.isThreadActive(), storeToS3Service.getConsumedStreamsCount(), StringFormatter.byteSize(storeToS3Service.getLastConsumedStreamSize()),
				StringFormatter.byteSize(storeToS3Service.getConsumedBytes()), s3ClientWrapper.getPartNumber()
		);
	}

	private String fsH() {
		if (fsNotActive()) {
			return "";
		}
		return String.format("|%6s|%12s",
				"Active", "Consumed"
		);
	}


	private String fs() {
		if (fsNotActive()) {
			return "";
		}
		return String.format("\u001b[31m|%6s|%12d\u001b[39m",
				storeToFile.isThreadActive(), storeToFile.getConsumedStreamCount()
		);
	}


	public double calcRate() {
		double duration = (double) (System.nanoTime() - startSystemNanos) / 1_000_000_000L;
		if (duration < 5) {
			return lastRate;
		}
		startSystemNanos = System.nanoTime();
		long totalWriteOps = dataFetcher.getLinesRead() - readCount;
		readCount = dataFetcher.getLinesRead();
		double rate = (double) totalWriteOps / duration;
		if (rate > 0) {
			lastRate = rate;
		}
		return lastRate;
	}
}
