package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.DataFetcher;

public class AwsKeyspaceFetchCountView extends ActionInThread {

	private final DataFetcher dataFetcher;

	public AwsKeyspaceFetchCountView(DataFetcher dataFetcher) {
		super("Counting", "Test AWS Keyspace fetch&count");
		this.dataFetcher = dataFetcher;
	}

	@Override
	public void execute() {
		int countAll = dataFetcher.iterateAndCountAll();
		print("countAll: " + countAll);
	}

}
