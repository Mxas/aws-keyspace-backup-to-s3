package lt.mk.awskeyspacebackuptos3.s3.storing;

import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBool;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofBytes;
import static lt.mk.awskeyspacebackuptos3.statistic.StringFormatter.ofNum;

import lt.mk.awskeyspacebackuptos3.statistic.StatProvider;

public class StoreToS3StatProvider implements StatProvider {

	private static final String[] COLUMNS = {"Active", "Consumed", "Last", "Total", "Part No"};
	private final StoreToS3Service item;
	private boolean wasOne;

	public StoreToS3StatProvider(StoreToS3Service item) {
		this.item = item;
	}

	@Override
	public String h1() {
		return "S3 Lines Storing";
	}

	@Override
	public String[] h2() {
		return COLUMNS;
	}

	@Override
	public String[] data() {
		return new String[]{
				ofBool(item.isThreadActive()),
				ofNum(item.getConsumedStreamsCount(), 4),
				ofBytes(item.getLastConsumedStreamSize()),
				ofBytes(item.getConsumedBytes()),
				ofNum(item.getPartNumber(), 4)
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