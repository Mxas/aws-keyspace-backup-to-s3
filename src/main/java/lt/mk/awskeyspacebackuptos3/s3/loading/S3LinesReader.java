package lt.mk.awskeyspacebackuptos3.s3.loading;

import java.io.BufferedReader;
import java.util.concurrent.atomic.LongAdder;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;
import lt.mk.awskeyspacebackuptos3.s3.S3ClientWrapper;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;
import lt.mk.awskeyspacebackuptos3.statistic.Statistical;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class S3LinesReader implements Statistical {

	private final S3ClientWrapper clientWrapper;
	private final DataQueue queue;

	private final LongAdder count =new LongAdder();
	private Thread thread;


	public S3LinesReader(S3ClientWrapper clientWrapper, DataQueue queue) {
		this.clientWrapper = clientWrapper;
		this.queue = queue;
	}


	public void startReading() {

		clientWrapper.initClient();

		count.reset();

		this.thread = ThreadUtil.newThreadStart(() -> {

			try (BufferedReader reader = clientWrapper.getNewFileReader()) {

				String firstLine = reader.readLine();

				System.out.println("First line: " + firstLine);
				queue.put(firstLine);
				String s;
				while ((s = reader.readLine()) != null) {
					count.increment();
					queue.put(s);
				}

				System.out.println("While file reading done");
				queue.dataLoadingFinished();

			} catch (Exception e) {
				e.printStackTrace();
			}

		}, "reading");
	}

	public long getTotalCount() {
		return count.longValue();
	}

	public void close() {
		ThreadUtil.stop(thread);
	}

	public boolean isThreadActive() {
		return ThreadUtil.isActive(thread);
	}

	@Override
	public StatProvider provider() {
		return new S3LinesReaderStatProvider(this);
	}
}
