package lt.mk.awskeyspacebackuptos3.keyspace.reinsert;

import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBool;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofNum;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofRate;

import lt.mk.awskeyspacebackuptos3.statistic.RateCalc;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;

public class ReinsertStatistic implements StatProvider {

	private static final String[] COLUMNS = {"Active", "Queue size", "Page", "Er.pages", "Reinserted", "Rate"};

	private final RateCalc rateCalc;
	private final ReinsertDataInvoker invoker;
	private boolean wasOne;

	public ReinsertStatistic(ReinsertDataInvoker invoker) {
		this.invoker = invoker;
		rateCalc = new RateCalc(invoker::getReinsertedCount);
	}

	@Override
	public String h1() {
		return "Aws Keyspace data insertion";
	}

	@Override
	public String[] h2() {
		return COLUMNS;
	}

	@Override
	public String[] data() {
		return new String[]{
				ofBool(invoker.isThreadActive()),
				ofNum(invoker.getQueueSize(), 9),
				ofNum(invoker.getPageCounter(), 9),
				ofNum(invoker.getErrorPagesCounter(), 9),
				ofNum(invoker.getReinsertedCount(), 12),
				ofRate(rateCalc.calcRate()),
		};
	}

	@Override
	public boolean on() {
		if (invoker.isThreadActive()) {
			wasOne = true;
		}
		return this.wasOne;
	}
}
