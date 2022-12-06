package lt.mk.awskeyspacebackuptos3.s3.loading;

import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBool;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofNum;

import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;

public class S3LinesReaderStatProvider implements StatProvider {

	private static final String[] COLUMNS = {"Active", "Total"};
	private final S3LinesReader item;
	private boolean wasOne;

	public S3LinesReaderStatProvider(S3LinesReader s3LinesReader) {
		this.item = s3LinesReader;
	}

	@Override
	public String h1() {
		return "S3 Lines reader";
	}

	@Override
	public String[] h2() {
		return COLUMNS;
	}

	@Override
	public String[] data() {
		return new String[]{
				ofBool(item.isThreadActive()),
				ofNum(item.getTotalCount(), 9),
		};
	}

	@Override
	public boolean on() {
		if (item.isThreadActive()) {
			wasOne = true;
		}
		return this.wasOne;
	}
}
