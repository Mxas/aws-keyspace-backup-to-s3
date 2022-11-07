package lt.mk.awskeyspacebackuptos3.statistic;

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

	public StatisticProvider(DataQueue queue, InputStreamProvider streamProvider, StoreToFile storeToFile, DataFetcher dataFetcher, StoreToS3Service storeToS3Service,
			S3ClientWrapper s3ClientWrapper) {
		this.queue = queue;
		this.streamProvider = streamProvider;
		this.storeToFile = storeToFile;
		this.dataFetcher = dataFetcher;
		this.storeToS3Service = storeToS3Service;
		this.s3ClientWrapper = s3ClientWrapper;
	}


	public String formatLine() {
		return dataFetcher() + memoryQueue() + s3() + fs() + "|";
	}

	private String dataFetcher() {
		return String.format("|%s|%d|%d",
				dataFetcher.isThreadActive(), dataFetcher.getPage(), dataFetcher.getLinesRead()
		);
	}

	private String memoryQueue() {
		return String.format("|%d|%d|%d|%d",
				queue.size(), streamProvider.getStreamCount(), streamProvider.getLinceCount(), streamProvider.getBytesCount()
		);
	}

	private String s3() {
		return String.format("|%s|%d|%d|%d|%d",
				storeToS3Service.isThreadActive(), storeToS3Service.getConsumedStreamsCount(), storeToS3Service.getLastConsumedStreamSize(),
				storeToS3Service.getConsumedBytes(), s3ClientWrapper.getPartNumber()
		);
	}

	private String fs() {
		return String.format("|%s|%d",
				storeToFile.isThreadActive(), storeToFile.getConsumedStreamCount()
		);
	}
}
