package lt.mk.awskeyspacebackuptos3.thread;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ThreadUtil {

    private static List<Thread> threads = new ArrayList<>();

    public static void await(CountDownLatch latch, int v, TimeUnit unit) {
        wrap(() -> latch.await(v, unit));
    }

    public static void sleep3s() {
        wrap(() -> Thread.sleep(3_000L));
    }

	public static boolean isActive(Thread thread) {
		return thread != null && thread.isAlive();
	}

    public static void stop(Thread thread) {
        try {
            if (thread != null) {
                thread.interrupt();
                thread.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public interface Vo {
        void vo() throws InterruptedException;
    }

    public interface Ro<R> {
        R ro() throws InterruptedException;
    }

    public static void sleep1s() {
        wrap(() -> Thread.sleep(1000L));
    }

    public static void wrap(Vo v) {
        try {
            v.vo();
        } catch (InterruptedException ignore) {
        }
    }

    public static <T> Optional<T> wrap(Ro<T> v) {
        try {
            return Optional.ofNullable(v.ro());
        } catch (InterruptedException ignore) {
        }
        return Optional.empty();
    }

    public static Thread newThreadStart(Runnable runnable, String name) {
        Thread thread = newThread(runnable, name);
        thread.start();
        return thread;
    }

    public static Thread newThread(Runnable runnable, String name) {
        Thread t = new Thread(() -> {
            runnable.run();
            threads.remove(Thread.currentThread());
        }, name);
        threads.add(t);
        return t;
    }

    public static void stopThreads() {
        try {
            for (Thread thread : threads) {
                thread.interrupt();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
