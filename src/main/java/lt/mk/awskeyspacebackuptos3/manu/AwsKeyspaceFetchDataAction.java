package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.DataFetcher;

public class AwsKeyspaceFetchDataAction extends ActionInThread {

	private final DataFetcher dataFetcher;

	public AwsKeyspaceFetchDataAction(DataFetcher dataFetcher) {
		super("Fetching data from keyspace", "Start 'AWS Keyspace data fetching' thread");
		this.dataFetcher = dataFetcher;
	}

	@Override
	public void execute() {
		dataFetcher.startReading();
	}

}
