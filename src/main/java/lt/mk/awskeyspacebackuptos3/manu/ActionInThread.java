package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.ActionView;

public abstract class ActionInThread extends ActionView {

	public ActionInThread(String runningTitle, String nameInParentMenu) {
		super(runningTitle, nameInParentMenu);
	}

	@Override
	public void executeCustomAction() {
		try {
			Thread t = new Thread(this::execute);
			t.start();
			t.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	abstract void execute();
}
