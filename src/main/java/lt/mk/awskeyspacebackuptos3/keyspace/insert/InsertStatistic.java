package lt.mk.awskeyspacebackuptos3.keyspace.insert;

import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBool;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofNum;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofRate;

import lt.mk.awskeyspacebackuptos3.statistic.RateCalc;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;

public class InsertStatistic implements StatProvider {

	private static final String[] COLUMNS = {"Active", "Queue size", "Lines inserted", "Rate"};

	private final RateCalc rateCalc;
	private final InsertInvoker invoker;
	private boolean wasOne;

	public InsertStatistic(InsertInvoker invoker) {
		this.invoker = invoker;
		rateCalc = new RateCalc(invoker::getLinesInserted);
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
				ofNum(invoker.getLinesInserted(), 12),
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
