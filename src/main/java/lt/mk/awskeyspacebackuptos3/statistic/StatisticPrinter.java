
package lt.mk.awskeyspacebackuptos3.statistic;

import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class StatisticPrinter {

	public final StatisticProvider statisticProvider;
	private Timer timer;
	private String lastPrintedLine;

	public StatisticPrinter(StatisticProvider statisticProvider) {
		this.statisticProvider = statisticProvider;
	}


	public void iniStatPrinting() {
		try {
			printEmpty();
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

	private void printEmpty() {
		System.out.println();
		System.out.println();
	}

	private void printLine() {
		String line = statisticProvider.dataLine();
		printInSameLIne(line);
	}

	private void printInSameLIne(String line) {
		if (!Objects.equals(this.lastPrintedLine, line)) {
			this.lastPrintedLine = line;
			System.out.print("\r" + line);
		}
	}

	public void close() {
		try {
			timer.cancel();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
