package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.s3.StoreToS3TestService;

public class SaveToTestS3Action extends ActionInThread {

	private final StoreToS3TestService testService;

	public SaveToTestS3Action(StoreToS3TestService testService) {
		super("Testing S3 connectivity", "Test AWS S3");
		this.testService = testService;
	}

	@Override
	public void execute() {
		testService.test();
	}

}
