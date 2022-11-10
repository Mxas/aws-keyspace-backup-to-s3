
package lt.mk.awskeyspacebackuptos3.statistic;

import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class StatisticPrinter {

	public final StatisticProvider statisticProvider;
	private Timer timer;
	private String lastPrintedLine;
	private boolean headerPrinted;

	public StatisticPrinter(StatisticProvider statisticProvider) {
		this.statisticProvider = statisticProvider;
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

		String line = statisticProvider.dataLine();
		printInSameLIne(line);
//		System.out.println(statisticProvider.dataLine());
	}

	private void printEmpty() {
		System.out.println();
		System.out.println();
	}

	private void printHeader() {
		System.out.println();
		String headerLine1 = statisticProvider.headerLine1();
		space(headerLine1.length());
		System.out.println(headerLine1);
		space(headerLine1.length());
		System.out.println(statisticProvider.headerLine2());
		space(headerLine1.length());
//		System.out.println();
	}

	private void space(int length) {
		for (int i = 0; i < length; i++) {
			System.out.print("-");
		}
		System.out.println();
	}

	private void printInSameLIne(String line) {
		if (!Objects.equals(this.lastPrintedLine, line)) {
//			printIfNeededHeader();
			this.lastPrintedLine = line;
			System.out.print("\r" + line);
		}
	}

	private void printIfNeededHeader() {
		if (!this.headerPrinted) {
			headerPrinted = true;
			printEmpty();
			printHeader();
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
