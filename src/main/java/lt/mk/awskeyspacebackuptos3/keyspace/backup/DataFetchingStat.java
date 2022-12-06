package lt.mk.awskeyspacebackuptos3.keyspace.backup;

import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBool;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofNum;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofRate;

import lt.mk.awskeyspacebackuptos3.statistic.RateCalc;
import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;

public class DataFetchingStat implements StatProvider {

	private final DataFetcher dataFetcher;
	private final RateCalc rateCalc;
	private boolean wasOne;

	public DataFetchingStat(DataFetcher dataFetcher) {
		this.dataFetcher = dataFetcher;
		this.rateCalc = new RateCalc(dataFetcher::getLinesRead);
	}

	@Override
	public String h1() {
		return "AWS Keyspace Data Fetching";
	}

	@Override
	public String[] h2() {
		return new String[]{
				"Active", "Page No", "Total Lines", "Queue size", "Rate p/s"
		};
	}

	@Override
	public String[] data() {
		return new String[]{
				ofBool(dataFetcher.isThreadActive()),
				ofNum(dataFetcher.getPage(), 9),
				ofNum(dataFetcher.getLinesRead(), 12),
				ofNum(dataFetcher.getQueueSize(), 9),
				ofRate(rateCalc.calcRate()),
		};
	}

	@Override
	public boolean on() {
		if (dataFetcher.isThreadActive()) {
			wasOne = true;
		}
		return this.wasOne;
	}
}
