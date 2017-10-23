import org.agrona.concurrent.HighResolutionTimer;
import util.SingleThreadExecutor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

public class TestExecutor {
    public static final int NANOS_IN_SECOND = 1_000_000_000;
    public static final long COUNT_EVERY = 64 * 1024 * 1024;
    static final Charset CHARSET = Charset.forName("Latin1");

    public static void main(String[] args) throws InterruptedException {
        HighResolutionTimer.enable();

        SingleThreadExecutor processor = SingleThreadExecutor
                .createWithHighResolutionScheduledQueue(r -> new Thread(r, "processor"), false);

        while (true) {
            long start = System.nanoTime();

            CountDownLatch latch = new CountDownLatch(1);

            final PaddedMutableLong counter = new PaddedMutableLong();
            final PaddedMutableLong total = new PaddedMutableLong();

            for (long l = 0; l < COUNT_EVERY; l++) {
                long finalL = l;
                while (!processor.tryExecute(new AddingRunnable(total, finalL, counter, latch))) {
                    LockSupport.parkNanos(1);
                }
            }
            latch.await();
            long end = System.nanoTime();
            log((new BigDecimal(COUNT_EVERY).multiply(new BigDecimal(NANOS_IN_SECOND)).divide(new BigDecimal(end - start), 0, RoundingMode.HALF_UP)) + " msgs/s");

        }
    }

    private static void log(String msg) {
        System.out.println(Thread.currentThread().getName() + " | " + msg);
    }

    private static void trace(String msg) {
//        System.out.println(Thread.currentThread().getName() + " | " + msg);
    }

    private static class AddingRunnable implements Runnable {
        private final PaddedMutableLong total;
        private final long value;
        private final PaddedMutableLong counter;
        private final CountDownLatch latch;

        public AddingRunnable(PaddedMutableLong total, long value, PaddedMutableLong counter, CountDownLatch latch) {
            this.total = total;
            this.value = value;
            this.counter = counter;
            this.latch = latch;
        }

        @Override
        public void run() {
            total.add(value);
            if (counter.incrementAndGet() == COUNT_EVERY) {
                latch.countDown();
            }
        }
    }
}

