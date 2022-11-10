package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.MenuView;
import lt.mk.awskeyspacebackuptos3.SingletonManager;

public class TestingMenuView extends MenuView {

	public TestingMenuView(SingletonManager manager) {
		super("There can be tested connections/configuration to resources.", "Test configuration");
		addMenuItem(new AwsKeyspaceTestView(manager.getTestQueryHelper()));
		addMenuItem(new AwsKeyspaceFetchCountView(manager.getDataFetcher()));
		addMenuItem(new AwsKeyspaceHeaderView(manager.getTableHeaderReader()));
		addMenuItem(new SaveToTestS3Action(manager.getStoreToS3TestService()));
	}
}
