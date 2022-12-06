package lt.mk.awskeyspacebackuptos3;

import lt.mk.awskeyspacebackuptos3.cli.CLIInvoker;
import lt.mk.awskeyspacebackuptos3.manu.RootView;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public class Main {

	private static SingletonManager MANAGER;

	public static void main(String[] args) {

		Runtime.getRuntime().addShutdownHook(new Thread(Main::cleanUpAndClose));

		init(args);

		if (!MANAGER.getConfigurationHolder().menu.isMenuDisabled()) {
			new RootView(MANAGER).display();
			State.shutdown = true;
		} else {
			new CLIInvoker(MANAGER, MANAGER.getConfigurationHolder().menu).run();
		}
	}

	private static void init(String[] args) {
		MANAGER = new SingletonManager(args);
		MANAGER.init();
		MANAGER.getCliCommandLine().parse();
	}

	private static void cleanUpAndClose() {
		ThreadUtil.stopThreads();
		if (MANAGER != null) {
			MANAGER.close();
		}

		System.out.println("Exiting ...");
	}

}
