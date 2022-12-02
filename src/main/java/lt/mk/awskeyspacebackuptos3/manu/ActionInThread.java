package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.ActionView;
import lt.mk.awskeyspacebackuptos3.thread.ThreadUtil;

public abstract class ActionInThread extends ActionView {

	public ActionInThread(String runningTitle, String nameInParentMenu) {
		super(runningTitle, nameInParentMenu);
	}

	@Override
	public void executeCustomAction() {
		try {
			Thread t = ThreadUtil.newThreadStart(this::execute,"me");
			t.join();:::
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	abstract void execute();

	@Override
	public void display() {
		this.println();
		this.println(this.runningTitle);
		this.executeCustomAction();
//		this.pause();
		this.goBack();
	}
}
