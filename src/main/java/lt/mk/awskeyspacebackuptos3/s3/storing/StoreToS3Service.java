package lt.mk.awskeyspacebackuptos3.s3.storing;

import java.io.IOException;
import java.io.InputStream;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.inmemory.InputStreamProvider;
import lt.mk.awskeyspacebackuptos3.s3.S3ClientWrapper;
import lt.mk.awskeyspacebackuptos3.s3.SyncS3MultipartUploader;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;
import lt.mk.awskeyspacebackuptos3.statistic.Statistical;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class StoreToS3Service implements Statistical {

	private final S3ClientWrapper client;
	private final InputStreamProvider streamProvider;
	private final SyncS3MultipartUploader uploader;

	private Thread thread;
	private int consumedStreamsCount;
	private int lastConsumedStreamSize;
	private long consumedBytes;

	public StoreToS3Service(S3ClientWrapper client, InputStreamProvider streamProvider, SyncS3MultipartUploader uploader) {
		this.client = client;
		this.streamProvider = streamProvider;
		this.uploader = uploader;
	}

	public void startStoring() {
		try {
			client.initClient();
			client.initMultipartUploader();
			consumedStreamsCount = 0;
			lastConsumedStreamSize = -1;
			consumedBytes = 0;

			if (isThreadActive()) {
				System.out.println("'StoreToS3' Already running");
			} else {
				thread = ThreadUtil.newThread(() -> StartWriteToS3(), "StoreToS3");
				thread.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean isThreadActive() {
		return thread != null && thread.isAlive();
	}

	private void StartWriteToS3() {

		try {
			InputStream is;

			do {
				is = streamProvider.getStream();
				if (!hasData(is)) {
					break;
				}
				consumedStreamsCount++;
				lastConsumedStreamSize = is.available();
				consumedBytes = consumedBytes + is.available();
				uploader.upload(is);

			} while (State.isRunning());
			uploader.waitFullComplete();

			System.out.println("Storing finished ...");

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean hasData(InputStream is) throws IOException {
		return is != null && is.available() > 0;
	}

	public int getConsumedStreamsCount() {
		return consumedStreamsCount;
	}

	public int getLastConsumedStreamSize() {
		return lastConsumedStreamSize;
	}

	public long getConsumedBytes() {
		return consumedBytes;
	}

	public long getPartNumber() {
		return client.getPartNumber();
	}

	@Override
	public StatProvider provider() {
		return new StoreToS3StatProvider(this);
	}
}
