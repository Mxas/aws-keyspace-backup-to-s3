package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.MenuView;
import lt.mk.awskeyspacebackuptos3.SingletonManager;

public class RootView extends MenuView {

	public RootView(SingletonManager manager) {
		super("Welcome to AWS Keyspace Backups Creation Tool.", "");
		addMenuItem(new AwsKeyspaceConfigAction(manager.getConfigurationHolder(), manager.getCqlSessionProvider()));
		addMenuItem(new AwsKeyspaceTestView(manager.getTestQueryHelper()));
		addMenuItem(new SaveToTestS3Action(manager.getStoreToS3TestService()));
		addMenuItem(new AwsKeyspaceHeaderView(manager.getTableHeaderReader()));
		addMenuItem(new AwsKeyspaceFetchCountView(manager.getDataFetcher()));
		addMenuItem(new AwsKeyspaceFetchDataAction(manager.getDataFetcher()));
		addMenuItem(new SaveToFileAction(manager.getStoreToFile()));
		addMenuItem(new SaveToS3Action(manager.getStoreToS3Service()));
		addMenuItem(new StatisticsView(manager.getStatisticPrinter()));
	}
}
