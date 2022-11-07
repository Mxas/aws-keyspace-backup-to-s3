package lt.mk.awskeyspacebackuptos3.fs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.Fs;
import lt.mk.awskeyspacebackuptos3.inmemory.InputStreamProvider;

public class StoreToFile {

	private final Fs config;
	private final InputStreamProvider streamProvider;
	private int consumedStreamCount;
	private Thread thread;


	public StoreToFile(Fs config, InputStreamProvider streamProvider) {
		this.config = config;
		this.streamProvider = streamProvider;
	}

	public void store() {
		if (isThreadActive()) {
			System.out.println("Already started");
		} else {
			thread = new Thread(this::writeToFile);
			thread.start();
		}
	}

	public boolean isThreadActive() {
		return thread != null && thread.isAlive();
	}

	private void writeToFile() {
		try (OutputStream writer = new FileOutputStream(config.storeTo, true)) {

			InputStream is;
			consumedStreamCount = 0;
			do {
				is = streamProvider.getStream();
				if (!hasData(is)) {
					break;
				}
				consumedStreamCount++;
				writer.write(is.readNBytes(is.available()));

			} while (true);

			writer.flush();
			System.out.println("Finished writing to " + config.storeTo);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean hasData(InputStream is) throws IOException {
		return is != null && is.available() > 0;
	}

	public int getConsumedStreamCount() {
		return consumedStreamCount;
	}
}
