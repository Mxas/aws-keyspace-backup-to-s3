
package lt.mk.awskeyspacebackuptos3.statistic;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.Stat;

public class StatisticPrinter {

	public final StatisticsRender statisticsRender;
	private final Stat config;
	private Timer timer;
	private String lastPrintedLine;
	private LocalDateTime lastPrintedHeader = LocalDateTime.now();
	private LocalDateTime lastNewLinePrinted = LocalDateTime.now();
	private LocalDateTime lastStatPrinted = LocalDateTime.now();

	public StatisticPrinter(StatisticsRender statisticsRender, Stat config) {
		this.statisticsRender = statisticsRender;
		this.config = config;
	}


	public void iniStatPrinting() {
		try {
			this.lastStatPrinted = LocalDateTime.now();
			this.lastNewLinePrinted = LocalDateTime.now();
			this.lastPrintedHeader = LocalDateTime.now();
			printEmpty();
			printHeader();
			printLine();

			timer = new Timer("StatisticPrinter");
			timer.schedule(new TimerTask() {
				public void run() {
					printLine();
				}
			}, 1000L, config.printStatisticInMillis);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printLine() {
		printInSameLIne(statisticsRender.data());
		checkForFinishing();
	}

	private void checkForReprintHeader() {
		if (Duration.between(lastPrintedHeader, LocalDateTime.now()).getSeconds() > config.printHeaderAfterSeconds) {
			printEmpty();
			printHeader();
		} else {
			if (Duration.between(lastNewLinePrinted, LocalDateTime.now()).getSeconds() > config.printStatNewLineAfterSeconds) {
				lastNewLinePrinted = LocalDateTime.now();
				System.out.println();
			}
		}
	}

	private void checkForFinishing() {
		if (Duration.between(lastNewLinePrinted, LocalDateTime.now()).getSeconds()  > config.stopStatsPrintingAfterNotChangedSeconds) {
			System.out.println("Stoppling statistics timer...");
			close();
		}
	}

	private void printEmpty() {
		System.out.println();
		System.out.println();
	}

	private void printHeader() {
		System.out.println(statisticsRender.header());
		lastPrintedHeader = LocalDateTime.now();
		lastNewLinePrinted = LocalDateTime.now();
	}


	private void printInSameLIne(String line) {

		if (!Objects.equals(this.lastPrintedLine, line)) {
			checkForReprintHeader();
			this.lastPrintedLine = line;
			System.out.print("\r" + line);
			lastStatPrinted = LocalDateTime.now();
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
