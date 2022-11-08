package lt.mk.awskeyspacebackuptos3.manu;

import lt.mk.awskeyspacebackuptos3.statistic.StatisticPrinter;

public class StatisticsView extends ActionInThread {

	private final StatisticPrinter statisticPrinter;

	public StatisticsView(StatisticPrinter statisticPrinter) {
		super("Statistics view", "View progress/Stats");
		this.statisticPrinter = statisticPrinter;
	}

	@Override
	public void execute() {

		statisticPrinter.iniStatPrinting();

		this.prompt("Any symbol to cancel", String.class);

		statisticPrinter.close();

	}

}
