package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.insert.InsertInvoker;

public class AwsKeyspaceInsertAction extends ActionInThread {

	private final InsertInvoker invoker;

	public AwsKeyspaceInsertAction(InsertInvoker invoker) {
		super("Insert from S3 to AWS keyspace", "Start 'AWS Keyspace data insert' thread");
		this.invoker = invoker;
	}

	@Override
	public void execute() {
		invoker.startR();
	}

}
