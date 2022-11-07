package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.s3.StoreToS3Service;

public class SaveToS3Action extends ActionInThread {

	private final StoreToS3Service storeService;

	public SaveToS3Action(StoreToS3Service storeService) {
		super("Saving to AWS S3", "Start 'Save to AWS S3' thread");
		this.storeService = storeService;
	}

	@Override
	public void execute() {
		storeService.startStoring();
	}

}
