package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.MenuView;
import lt.mk.awskeyspacebackuptos3.SingletonManager;

public class RootView extends MenuView {

	public RootView(SingletonManager manager) {
		super("Welcome to AWS Keyspace Backups Creation Tool.", "");
		addMenuItem(new ConfigAction(manager.getConfigurationHolder()));
		addMenuItem(new TestingMenuView(manager));
		addMenuItem(new AwsKeyspaceFetchDataAction(manager.getDataFetcher()));
		addMenuItem(new SaveToS3Action(manager.getStoreToS3Service()));
		addMenuItem(new SaveToFileAction(manager.getStoreToFile()));
		addMenuItem(new StatisticsView(manager.getStatisticPrinter()));
	}
}
