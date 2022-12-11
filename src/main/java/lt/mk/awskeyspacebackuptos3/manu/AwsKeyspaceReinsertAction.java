package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.keyspace.reinsert.ReinsertDataInvoker;

public class AwsKeyspaceReinsertAction extends ActionInThread {

	private final ReinsertDataInvoker reinsertDataInvoker;

	public AwsKeyspaceReinsertAction(ReinsertDataInvoker reinsertDataInvoker) {
		super("Reinsert AWS keyspace data", "Start 'AWS Keyspace reinsert' thread");
		this.reinsertDataInvoker = reinsertDataInvoker;
	}

	@Override
	public void execute() {
		reinsertDataInvoker.startReinserting();
	}

}
