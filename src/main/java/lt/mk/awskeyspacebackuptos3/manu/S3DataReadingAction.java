package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.s3.loading.S3LinesReader;

public class S3DataReadingAction extends ActionInThread {

	private final S3LinesReader s3LinesReader;

	public S3DataReadingAction(S3LinesReader s3LinesReader) {
		super("Reading from AWS S3", "Start 'Read from AWS S3 bucket' thread");
		this.s3LinesReader = s3LinesReader;
	}

	@Override
	public void execute() {
		s3LinesReader.startReading();
	}

}
