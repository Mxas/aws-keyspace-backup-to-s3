package lt.mk.awskeyspacebackuptos3.keyspace.delete;

import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBool;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofNum;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofRate;

import lt.mk.awskeyspacebackuptos3.statistic.RateCalc;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;

public class DeleteStatistic implements StatProvider {

	private static final String[] COLUMNS = {"Active", "Queue size", "Page No", "Lines read", "Delete threads", "Lines Deleted", "Rate"};

	private final RateCalc rateCalc;
	private final DeleteInvoker deleteInvoker;
	private boolean wasOne;

	public DeleteStatistic(DeleteInvoker deleteInvoker) {
		this.deleteInvoker = deleteInvoker;
		rateCalc = new RateCalc(deleteInvoker::getLinesDeleted);
	}

	@Override
	public String h1() {
		return "Aws Keyspace rows deletion";
	}

	@Override
	public String[] h2() {
		return COLUMNS;
	}

	@Override
	public String[] data() {
		return new String[]{
				ofBool(deleteInvoker.isThreadActive()),
				ofNum(deleteInvoker.getQueueSize(), 9),
				ofNum(deleteInvoker.getPage(), 9),
				ofNum(deleteInvoker.getLinesRead(), 12),
				ofNum(deleteInvoker.getDeleteThreadsCount(), 2),
				ofNum(deleteInvoker.getLinesDeleted(), 12),
				ofRate(rateCalc.calcRate()),
		};
	}

	@Override
	public boolean on() {
		if (deleteInvoker.isThreadActive()) {
			wasOne = true;
		}
		return this.wasOne;
	}
}
