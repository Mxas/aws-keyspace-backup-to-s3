package lt.mk.awskeyspacebackuptos3.statistic;

import java.util.function.Supplier;

public class RateCalc {

    private final Supplier<Long> val;

    private long startSystemNanos;
    private double lastRate;
    private long lastCount;

    public RateCalc(Supplier<Long> value) {
        this.val = value;
    }

    public double calcRate() {
        double duration = (double) (System.nanoTime() - startSystemNanos) / 1_000_000_000L;
        if (duration < 5) {
            return lastRate;
        }
        startSystemNanos = System.nanoTime();
        long val = this.val.get();
        long totalWriteOps = val - lastCount;
        lastCount = val;
        double rate = (double) totalWriteOps / duration;
        if (rate > 0) {
            lastRate = rate;
        }
        return lastRate;
    }
}
