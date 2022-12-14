package lt.mk.awskeyspacebackuptos3.inmemory;

import com.amazonaws.services.s3.internal.Constants;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Optional;
import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.InMemory;

public class InputStreamProvider {


	static int SIZE = 200 * Constants.MB;

	private final DataQueue queue;
	private final InMemory config;
	private int linceCount;
	private int streamCount;
	private int currentStreamSize;
	private long bytesCount;

	public InputStreamProvider(DataQueue queue, InMemory config) {
		this.queue = queue;
		this.config = config;
	}

	public void init() {
		this.linceCount = 0;
		this.streamCount = 0;
		this.bytesCount = 0;
		SIZE = config.singleStreamSizeInMB * Constants.MB;
	}

	public InputStream getStream() {
		try {

			ByteArrayOutputStream bufferOutputStream = new ByteArrayOutputStream();
			PrintWriter writer = new PrintWriter(bufferOutputStream);
			Optional<String> line;

			this.streamCount++;

			do {
				line = queue.poll();
				if (line.isPresent()) {
					writer.println(line.get());
					this.linceCount++;
				} else {
					System.out.println("Queue is empty");
				}
				this.currentStreamSize = bufferOutputStream.size();
				if (this.currentStreamSize > SIZE) {
					break;
				}
			} while ((!queue.isFinished() || line.isPresent()) && State.isRunning());
			if (line.isEmpty()) {
				System.out.println("Queue is empty exiting polling");
			}
			writer.flush();
			this.bytesCount = this.bytesCount + bufferOutputStream.size();
			this.currentStreamSize = 0;
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
		return this.bytesCount + this.currentStreamSize;
	}
}
