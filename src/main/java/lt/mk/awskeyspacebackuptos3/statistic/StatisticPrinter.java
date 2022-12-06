
package lt.mk.awskeyspacebackuptos3.statistic;

import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class StatisticPrinter {

	public final StatisticsRender statisticsRender;
	private Timer timer;
	private String lastPrintedLine;
	private boolean headerPrinted;

	public StatisticPrinter(StatisticsRender statisticsRender) {
		this.statisticsRender = statisticsRender;
	}


	public void iniStatPrinting() {
		try {
			printEmpty();
			printHeader();
			printLine();

			timer = new Timer("StatisticPrinter");
			timer.schedule(new TimerTask() {
				public void run() {
					printLine();
				}
			}, 1000L, 500L);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printLine() {

		String line = statisticsRender.data();
		printInSameLIne(line);
	}

	private void printEmpty() {
		System.out.println();
		System.out.println();
	}

	private void printHeader() {
		System.out.print(statisticsRender.header());

	}


	private void printInSameLIne(String line) {
		if (!Objects.equals(this.lastPrintedLine, line)) {
			this.lastPrintedLine = line;
			System.out.print("\r" + line);
		}
	}


	public void close() {
		try {
			if (timer != null) {
				timer.cancel();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
