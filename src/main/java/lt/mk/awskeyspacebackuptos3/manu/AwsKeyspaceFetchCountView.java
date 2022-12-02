package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.State;
import lt.mk.awskeyspacebackuptos3.keyspace.TestCountHelper;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

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
        this.thread = ThreadUtil.newThreadStart(() -> {

            while (running && State.isRunning()) {
                System.out.print("\rLines Count " + countHelper.getLinesRead() + ", pages " + countHelper.getPage() + " <press Enter to quit>");
                ThreadUtil.sleep1s();
            }

        }, "count");
    }

}
