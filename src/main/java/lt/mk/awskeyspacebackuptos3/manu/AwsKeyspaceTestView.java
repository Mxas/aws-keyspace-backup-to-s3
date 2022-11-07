package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.TestQueryHelper;

public class AwsKeyspaceTestView extends ActionInThread {

	private final TestQueryHelper testQueryHelper;

	public AwsKeyspaceTestView(TestQueryHelper testQueryHelper) {
		super("Testing AWS connectivity, selecting first row", "Test AWS Keyspace configuration");
		this.testQueryHelper = testQueryHelper;
	}

	@Override
	public void execute() {
		String rs = testQueryHelper.testOneJson();
		print("JSON: " + rs);
	}

}
