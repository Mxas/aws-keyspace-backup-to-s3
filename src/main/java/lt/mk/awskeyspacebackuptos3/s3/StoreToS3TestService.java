package lt.mk.awskeyspacebackuptos3.s3;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class StoreToS3TestService {


	private final S3ClientWrapper client;
	private final SyncS3MultipartUploader uploader;

	public StoreToS3TestService(S3ClientWrapper client, SyncS3MultipartUploader uploader) {
		this.client = client;
		this.uploader = uploader;
	}

	public void test() {
		testPutObject();
		writeToS3MultiPart();
	}

	private void testPutObject() {
		client.initClient();
		try {
			client.storeSimpleFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void writeToS3MultiPart() {
		System.out.println("Running: writeToS3MultiPart");
		client.initClient();
		client.initMultipartUploader();
		try {
			uploader.upload(new ByteArrayInputStream("one,two,three\n".getBytes(StandardCharsets.UTF_8)));
			uploader.waitFullComplete();
			System.out.println("Successfully: writeToS3MultiPart");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
