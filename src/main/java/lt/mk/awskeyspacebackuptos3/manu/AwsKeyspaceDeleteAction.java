package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.delete.DeleteInvoker;

public class AwsKeyspaceDeleteAction extends ActionInThread {

	private final DeleteInvoker deleteInvoker;

	public AwsKeyspaceDeleteAction(DeleteInvoker deleteInvoker) {
		super("Deleting from AWS keyspace", "Start 'AWS Keyspace data delete' thread");
		this.deleteInvoker = deleteInvoker;
	}

	@Override
	public void execute() {
		deleteInvoker.startR();
	}

}
