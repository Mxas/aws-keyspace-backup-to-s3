package lt.mk.awskeyspacebackuptos3.s3;

import static com.amazonaws.util.ValidationUtils.assertNotEmpty;
import static com.amazonaws.util.ValidationUtils.assertStringNotEmpty;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.S3;

public class S3ClientWrapper {

	private final S3 config;
	private final AwsKeyspaceConf keyspaceConfig;
	private AmazonS3 s3Client;
	private String uploadId;
	private String fileName;
	private AtomicInteger partNumberAdder = new AtomicInteger(0);

	private final DateTimeFormatter DATE_PATTERN = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");

	public S3ClientWrapper(S3 config, AwsKeyspaceConf keyspaceConfig) {
		this.config = config;
		this.keyspaceConfig = keyspaceConfig;
	}


	public void initClient() {
		assertStringNotEmpty(config.region, "S3 region not provided");
		assertStringNotEmpty(config.bucket, "S3 bucket not provided");
		Regions region = Regions.fromName(config.region);
		this.s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();

	}


	public void storeSimpleFile() {
		String fileNameThere = buildFileName();
		System.out.println();
		System.out.println("Running: storeSimpleFile");
		System.out.println("File name: " + fileNameThere);
		System.out.println("S3 bucket: " + config.bucket);
		System.out.println();
		s3Client.putObject(config.bucket, fileNameThere, new ByteArrayInputStream("one,two,three".getBytes(StandardCharsets.UTF_8)),
				new ObjectMetadata());
		System.out.println("Successfully: storeSimpleFile");

	}

	private String buildFileName() {
		String fileName = DATE_PATTERN.format(LocalDateTime.now()) + "_.csv";
		return config.folder + "/" + keyspaceConfig.keyspace + "/" + keyspaceConfig.table + "/" + fileName;
	}

	private ObjectMetadata getContentType() {
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentType("text/csv");
		return objectMetadata;
	}


	private ObjectTagging getTags() {
		Tag tag = new Tag("date", LocalDateTime.now().toString());
		return new ObjectTagging(Collections.singletonList(tag));
	}

	public void initMultipartUploader() {
		this.partNumberAdder = new AtomicInteger(0);
		this.fileName = buildFileName();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.bucket, fileName);
		initRequest.setObjectMetadata(getContentType());
		initRequest.setTagging(getTags());
		initRequest.setKey(fileName);

		this.uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
	}

	public UploadPartRequest initUploadPartRequest() {
		assertStringNotEmpty(this.fileName, "File name must be not empty");
		assertStringNotEmpty(this.uploadId, "Upload ID must be not empty");
		return new UploadPartRequest()
				.withBucketName(config.bucket)
				.withKey(this.fileName)
				.withUploadId(this.uploadId)
				.withPartNumber(partNumberAdder.incrementAndGet());
	}

	public UploadPartResult uploadPart(UploadPartRequest uploadRequest) {
		return s3Client.uploadPart(uploadRequest);
	}

	public void completeMultipartUpload(List<PartETag> partETags) {
		assertStringNotEmpty(this.fileName, "File name must be not empty");
		assertStringNotEmpty(this.uploadId, "Upload ID must be not empty");
		assertNotEmpty(partETags, "Parts collection is empty");

		CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(config.bucket, this.fileName, uploadId, partETags);
		s3Client.completeMultipartUpload(completeRequest);

		this.fileName = null;
		this.uploadId = null;
	}

	public int getPartNumber() {
		return partNumberAdder.get();
	}

	public String getFileName() {
		return fileName;
	}

	public String getUploadId() {
		return uploadId;
	}
}
