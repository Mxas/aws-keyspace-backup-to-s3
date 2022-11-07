package lt.mk.awskeyspacebackuptos3.s3;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class SyncS3MultipartUploader {

	private final S3ClientWrapper s3;

	private final List<PartETag> partETags = new ArrayList<>();

	public SyncS3MultipartUploader(S3ClientWrapper s3) {
		this.s3 = s3;
	}


	public void upload(InputStream inputStream) {

		try {
			UploadPartRequest uploadRequest = s3.initUploadPartRequest()
					.withInputStream(inputStream)
					.withPartSize(inputStream.available());

			partETags.add(s3.uploadPart(uploadRequest).getPartETag());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void waitFullComplete() {
		s3.completeMultipartUpload(partETags);
		partETags.clear();
	}
}
