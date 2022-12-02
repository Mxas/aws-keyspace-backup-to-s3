package lt.mk.awskeyspacebackuptos3;

import lt.mk.awskeyspacebackuptos3.manu.RootView;

public class Main {

	private static SingletonManager MANAGER;

	public static void main(String[] args) {

		Runtime.getRuntime().addShutdownHook(new Thread(Main::cleanUpAndClose));

		init(args);

		if (!MANAGER.getConfigurationHolder().menu.disableMenu) {
			try {
				Thread menuThread = new Thread(() -> new RootView(MANAGER).display());
				menuThread.start();
				menuThread.join();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		State.shutdown = true;
	}

	private static void init(String[] args) {
		MANAGER = new SingletonManager(args);
		MANAGER.init();
		MANAGER.getCliCommandLine().parse();
	}

	private static void cleanUpAndClose() {
		if (MANAGER != null) {
			MANAGER.close();
		}

		System.out.println("Exiting ...");
	}

}
