package lt.mk.awskeyspacebackuptos3.cli;

import lt.mk.awskeyspacebackuptos3.SingletonManager;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.Menu;

public class CLIInvoker {

	private final SingletonManager manager;
	private final Menu config;

	public CLIInvoker(SingletonManager manager, Menu menu) {
		this.manager = manager;
		this.config = menu;
	}

	public void run() {
		//backup, restore, reinsert, delete
		switch (config.command) {
			case "backup":
				manager.getDataFetcher().startReading();
				manager.getStoreToS3Service().startStoring();
				break;
			case "restore":
				manager.getS3LinesReader().startReading();
				manager.getInsertInvoker().startR();
				break;
			case "reinsert":
				manager.getReinsertDataInvoker().startReinserting();
				break;
			case "delete":
				manager.getDeleteInvoker().startR();
				break;
			default:
				throw new IllegalArgumentException("Wrong command! Available: restore, backup, reinsert and delete.");
		}

		manager.getStatisticPrinter().iniStatPrinting();
	}
}
