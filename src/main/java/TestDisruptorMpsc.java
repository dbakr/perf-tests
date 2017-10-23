import com.lmax.disruptor.*;
import org.agrona.concurrent.HighResolutionTimer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

public class TestDisruptorMpsc {
    public static final BigDecimal NANOS_IN_SECOND = new BigDecimal("1000000000");
    public static final int BATCH_SIZE = 1;

    public static void main(String[] args) throws InterruptedException {
        HighResolutionTimer.enable();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024 * 64;

        RingBuffer<LongEvent> ringBuffer = RingBuffer.createMultiProducer(new LongEventEventFactory(), bufferSize,
//                new BusySpinWaitStrategy()
                new YieldingWaitStrategy()
        );
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        final int countEvery = 100_000_000;
        ValueAdditionEventHandler valueAdditionEventHandler = new ValueAdditionEventHandler();
        BatchEventProcessor<LongEvent> batchEventProcessor = new BatchEventProcessor<>(ringBuffer, sequenceBarrier,
                valueAdditionEventHandler);

        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());

        new Thread(batchEventProcessor, "processor").start();


        while (true) {
            long start = System.nanoTime();

            CountDownLatch latch = new CountDownLatch(1);
            valueAdditionEventHandler.reset(latch, countEvery);

            for (long l = 0; l < countEvery; ) {

                long hi = ringBuffer.next(BATCH_SIZE);
                for (int i = BATCH_SIZE - 1; i >= 0; i--) {
                    ringBuffer.get(hi - i).setValue(l);
                    ringBuffer.publish(hi - i);
                    l++;
                }
            }

            latch.await();

            long end = System.nanoTime();
            log((new BigDecimal(countEvery).multiply(NANOS_IN_SECOND).divide(new BigDecimal(end - start), 2, RoundingMode.HALF_UP)) + " msgs/s");

        }
    }

    private static void log(String msg) {
        System.out.println(Thread.currentThread().getName() + " | " + msg);
    }

    private static void trace(String msg) {
//        System.out.println(Thread.currentThread().getName() + " | " + msg);
    }

    private static class LongEventEventFactory implements EventFactory<LongEvent> {
        @Override
        public LongEvent newInstance() {
            return new LongEvent();
        }
    }
}

