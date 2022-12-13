package lt.mk.awskeyspacebackuptos3.cli;

import java.io.File;
import java.io.PrintStream;
import lt.mk.awskeyspacebackuptos3.SingletonManager;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.Menu;
import org.apache.commons.lang3.StringUtils;

public class CLIInvoker {

	private final SingletonManager manager;
	private final Menu config;

	public CLIInvoker(SingletonManager manager, Menu menu) {
		this.manager = manager;
		this.config = menu;
	}

	public void run() {
		checkOutputStream();

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

	private void checkOutputStream() {
		if (StringUtils.isNotBlank(config.outputFilePath)) {
//			try {
//				File file = new File(config.outputFilePath);
//				if (!file.exists()) {
//					file.createNewFile();
//				}
//				//Instantiating the PrintStream class
//				PrintStream stream = new PrintStream(file);
//				System.out.println("From now on " + file.getAbsolutePath() + " will be your console");
//				System.setOut(stream);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
		}
	}
}
