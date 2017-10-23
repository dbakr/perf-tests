import com.lmax.disruptor.*;
import org.agrona.concurrent.HighResolutionTimer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

public class TestDisruptor {
    public static final BigDecimal NANOS_IN_SECOND = new BigDecimal("1000000000");
    public static final int BATCH_SIZE = 1;
    static final Charset CHARSET = Charset.forName("Latin1");
//    static final ImmutableList<?> TEST_OBJECT = ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

    public static void main(String[] args) throws InterruptedException {
        HighResolutionTimer.enable();
//        final FSTConfiguration fst = EventBusMessage.newFstConfiguration();
        // Executor that will be used to construct new threads for consumers
//        Executor executor = Executors.newCachedThreadPool();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024 * 64;

        RingBuffer<LongEvent> ringBuffer = RingBuffer.createSingleProducer(new LongEventEventFactory(), bufferSize,
//                new BusySpinWaitStrategy()
                new WaitStrategy() {
                    private static final int SPIN_TRIES = 100;

                    @Override
                    public long waitFor(
                            final long sequence, Sequence cursor, final Sequence dependentSequence, final SequenceBarrier barrier)
                            throws AlertException, InterruptedException {
                        long availableSequence;
                        int counter = SPIN_TRIES;

                        while ((availableSequence = dependentSequence.get()) < sequence) {
                            counter = applyWaitMethod(barrier, counter);
                        }

                        return availableSequence;
                    }

                    @Override
                    public void signalAllWhenBlocking() {
                    }

                    private int applyWaitMethod(final SequenceBarrier barrier, int counter)
                            throws AlertException {
                        barrier.checkAlert();

                        if (0 == counter) {
                            Thread.yield();
//                            Thread.onSpinWait();
                        } else {
                            --counter;
                        }

                        return counter;
                    }
                }
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
//            bb.putLong(0, l);

                long hi = ringBuffer.next(BATCH_SIZE);
                for (int i = BATCH_SIZE - 1; i >= 0; i--) {
                    ringBuffer.get(hi - i).setValue(l);
                    ringBuffer.publish(hi - i);
                    l++;
                }

            /*if (!ringBuffer.tryPublishEvent((event, sequence) -> {
                trace("translating " + finalL);
                event.set(finalL);
//                event.set(buffer.getLong(0));
            })) {
                log("publish failed: " + l);
            } else {
                trace("published " + l);
            }*/
//            Thread.sleep(1);
//            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
//            LockSupport.parkNanos(10);
//            Thread.sleep(1);
            }

            latch.await();

            long end = System.nanoTime();
            log((new BigDecimal(countEvery).multiply(NANOS_IN_SECOND).divide(new BigDecimal(end - start), 2, RoundingMode.HALF_UP)) + " msgs/s");

        }
    }
//
//    private static Object compute(FSTConfiguration fstConfiguration, Long aLong) {
////        System.out.println("Processing " + aLong + " at " + Thread.currentThread().getName());
//        //        Thread t = Thread.currentThread();
//
////        double pow1 = Math.pow(integer, 0.2);
////        double pow2 = Math.pow(integer, pow1);
////        double pow3 = Math.pow(pow2, 0.4);
////        double pow4 = Math.pow(integer, pow3);
//
////        return conf.get().asByteArray(ImmutableList.of(pow1, pow2, pow3, pow4));
////        FSTConfiguration fstConfiguration = ((ComputationThread)t).fst;
//
//
////        FSTConfiguration fstConfiguration = conf.get();
//        int[] length = new int[1];
////        return fstConfiguration.asByteArray(TEST_OBJECT);
//        return new String(fstConfiguration.asSharedByteArray(TEST_OBJECT, length), 0, length[0], CHARSET);
//
////        return new byte[0];
////        return new String("1");
//
////                return fstConfiguration.asByteArray(TEST_OBJECT);
//    }

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

class LongEvent {
    private long value;

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "LongEvent{" +
                "value=" + value +
                '}';
    }
}

class ValueAdditionEventHandler implements EventHandler<LongEvent> {
    private final PaddedMutableLong value = new PaddedMutableLong();
    private long count;
    private CountDownLatch latch;

    public long getValue() {
        return value.getValue();
    }

    public void reset(final CountDownLatch latch, final long expectedCount) {
        value.setValue(0L);
        this.latch = latch;
        count = expectedCount;
    }

    @Override
    public void onEvent(final LongEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        value.add(event.getValue());
        if (count - 1 == (sequence % count)) {
            latch.countDown();
        }
    }
}