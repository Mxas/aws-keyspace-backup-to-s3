
package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.TableHeaderReader;

public class AwsKeyspaceHeaderView extends ActionInThread {

	private final TableHeaderReader headerReader;

	public AwsKeyspaceHeaderView(TableHeaderReader headerReader) {
		super("Test AWS Keyspace table/query header", "First row headers");
		this.headerReader = headerReader;
	}

	@Override
	public void execute() {
		print("Header: " + headerReader.readAndSetHeaders());
	}

}
