package lt.mk.awskeyspacebackuptos3.statistic;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import lt.mk.awskeyspacebackuptos3.fs.StoreToFile;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.inmemory.InputStreamProvider;
import lt.mk.awskeyspacebackuptos3.keyspace.DataFetcher;
import lt.mk.awskeyspacebackuptos3.s3.S3ClientWrapper;
import lt.mk.awskeyspacebackuptos3.s3.StoreToS3Service;

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


	public String headerLine() {
		return dataFetcherH() + memoryQueue() + s3() + fs() + "|";
	}

	public String dataLine() {
		return dataFetcher() + memoryQueue() + s3() + fs() + "|";
	}

	private String dataFetcherH() {
		return String.format("|%12s|%10s|%12s",
				"Data Fetching", "Page No", "Total Lines"
		);
	}

	private String dataFetcher() {
		return String.format("\u001b[32m|Data Fetching %s|Page No %d|Total Lines %d|Rate %.2f\u001b[39m",
				dataFetcher.isThreadActive(), dataFetcher.getPage(), dataFetcher.getLinesRead(), calcRate()
		);
	}

	private String memoryQueue() {
		return String.format("\u001b[33m|Queue size %d|Streams returned %d|Total Lines %d| Total size %s\u001b[39m",
				queue.size(), streamProvider.getStreamCount(), streamProvider.getLinceCount(), byteSize(streamProvider.getBytesCount())
		);
	}

	private String s3() {
		if (!storeToS3Service.isThreadActive() && storeToS3Service.getConsumedStreamsCount() < 1) {
			return "";
		}
		return String.format("\u001b[31m|S3 storing %s|Consumed Streams %d|Last Consumed Stream Size %s|Total size %s|Part No %d\u001b[39m",
				storeToS3Service.isThreadActive(), storeToS3Service.getConsumedStreamsCount(), byteSize(storeToS3Service.getLastConsumedStreamSize()),
				byteSize(storeToS3Service.getConsumedBytes()), s3ClientWrapper.getPartNumber()
		);
	}

	private String fs() {
		if (!storeToFile.isThreadActive() && storeToFile.getConsumedStreamCount() < 1) {
			return "";
		}
		return String.format("\u001b[31m|Storing to file %s|Consumed Stream Count %d\u001b[39m",
				storeToFile.isThreadActive(), storeToFile.getConsumedStreamCount()
		);
	}


	public static String byteSize(long bytes) {
		if (-1000 < bytes && bytes < 1000) {
			return bytes + " B";
		}
		CharacterIterator ci = new StringCharacterIterator("kMGTPE");
		while (bytes <= -999_950 || bytes >= 999_950) {
			bytes /= 1000;
			ci.next();
		}
		return String.format("%.1f %cB", bytes / 1000.0, ci.current());
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
