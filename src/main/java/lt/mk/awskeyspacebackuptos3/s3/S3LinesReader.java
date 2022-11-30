package lt.mk.awskeyspacebackuptos3.s3;

import java.io.BufferedReader;
import java.util.concurrent.atomic.LongAdder;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.S3;
import lt.mk.awskeyspacebackuptos3.inmemory.DataQueue;

public class S3LinesReader {

	private final S3ClientWrapper clientWrapper;
	private final DataQueue queue;

	private LongAdder count;


	public S3LinesReader(S3ClientWrapper clientWrapper,DataQueue queue) {
		this.clientWrapper = clientWrapper;
		this.queue = queue;
	}


	public void startReading() {

		clientWrapper.initClient();


		count = new LongAdder();

		new Thread(() -> {

			try (BufferedReader reader = clientWrapper.getNewFileReader()) {

				String firstLine = reader.readLine();

				System.out.println("First line: " + firstLine);

				String s;
				while ((s = reader.readLine()) != null) {
					count.increment();
					queue.put(s);
				}

				System.out.println("While file reading done");

			} catch (Exception e) {
				e.printStackTrace();
			}

		}).start();
	}

	public void close() {

	}
}
