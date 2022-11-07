package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.fs.StoreToFile;

public class SaveToFileAction extends ActionInThread {

	private final StoreToFile storeToFile;

	public SaveToFileAction(StoreToFile storeToFile) {
		super("Saving to file", "Start 'Save to File' thread");
		this.storeToFile = storeToFile;
	}

	@Override
	public void execute() {
		storeToFile.store();
	}

}
