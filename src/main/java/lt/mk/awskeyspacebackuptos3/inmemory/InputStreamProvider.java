package lt.mk.awskeyspacebackuptos3.inmemory;

import com.amazonaws.services.s3.internal.Constants;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;

public class InputStreamProvider {


	final int SIZE = 10 * Constants.MB;

	private final DataQueue queue;

	private int linceCount;
	private int streamCount;
	private long bytesCount;

	public InputStreamProvider(DataQueue queue) {
		this.queue = queue;
	}

	public void init() {
		this.linceCount = 0;
		this.streamCount = 0;
		this.bytesCount = 0;
	}

	public InputStream getStream() {
		try {

			ByteArrayOutputStream bufferOutputStream = new ByteArrayOutputStream();
			PrintWriter writer = new PrintWriter(bufferOutputStream);
			String line;

			this.streamCount++;

			do {
				line = queue.poll();
				if (line != null) {
					writer.println(line);
					this.linceCount++;
				}
				if (bufferOutputStream.size() > SIZE) {
					break;
				}
			} while (line != null);
			if (line == null) {
				System.out.println("Queue is empty exiting polling");
			}
			this.bytesCount = this.bytesCount + bufferOutputStream.size();
			return new ByteArrayInputStream(bufferOutputStream.toByteArray());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public int getLinceCount() {
		return linceCount;
	}

	public int getStreamCount() {
		return streamCount;
	}

	public long getBytesCount() {
		return bytesCount;
	}
}
