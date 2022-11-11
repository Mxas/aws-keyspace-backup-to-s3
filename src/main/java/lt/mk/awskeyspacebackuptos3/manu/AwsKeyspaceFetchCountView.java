package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.TestCountHelper;

public class AwsKeyspaceFetchCountView extends ActionInThread {

	private final TestCountHelper countHelper;
	private Thread thread;
	private boolean running;

	public AwsKeyspaceFetchCountView(TestCountHelper countHelper) {
		super("Counting", "Test AWS Keyspace fetch&count");
		this.countHelper = countHelper;
	}

	@Override
	public void execute() {
		countHelper.startCounting();

		startReading();

		pause();

		this.running = false;
		countHelper.close();
	}

	private void startReading() {
		this.running = true;
		this.thread = new Thread(() -> {

			while (running) {
				System.out.print("\rLines Count " + countHelper.getLinesRead() + ", pages " + countHelper.getPage() + " <press Enter to quit>");
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

		});
		this.thread.start();
	}

}
