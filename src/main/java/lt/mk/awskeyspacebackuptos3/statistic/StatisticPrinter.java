
package lt.mk.awskeyspacebackuptos3.statistic;

import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.LongAdder;

public class StatisticPrinter {

	public final StatisticsRender statisticsRender;
	private final LongAdder count = new LongAdder();
	private final LongAdder noChangesCount = new LongAdder();
	private Timer timer;
	private String lastPrintedLine;
	private boolean headerPrinted;

	public StatisticPrinter(StatisticsRender statisticsRender) {
		this.statisticsRender = statisticsRender;
	}


	public void iniStatPrinting() {
		try {
			count.reset();
			noChangesCount.reset();
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
		checkForReprintHeader();
		printInSameLIne(statisticsRender.data());
		checkForFinishing();
	}

	private void checkForReprintHeader() {
		count.increment();
		if (count.intValue() % 120 == 0) {
			printEmpty();
			printHeader();
		}
	}

	private void checkForFinishing() {
		if (noChangesCount.intValue() > 1000) {
			close();
		}
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
			noChangesCount.reset();
		} else {
			noChangesCount.increment();
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
