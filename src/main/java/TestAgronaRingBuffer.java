import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.HighResolutionTimer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class TestAgronaRingBuffer {
    private static final int NANOS_IN_SECOND = 1_000_000_000;

    public static void main(String[] args) throws InterruptedException {
        HighResolutionTimer.enable();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024 * 64;

        RingBuffer queue = new OneToOneRingBuffer(new UnsafeBuffer(ByteBuffer.allocate(bufferSize + TRAILER_LENGTH)));

        Runnable runnable = () -> {
            PaddedMutableLong counter = new PaddedMutableLong();
            PaddedMutableLong startTime = new PaddedMutableLong();
            PaddedMutableLong total = new PaddedMutableLong();

            final long countEvery = 64 * 1024 * 1024;
            final long countEveryMask = countEvery - 1;

            startTime.setValue(System.nanoTime());

            final BigInteger nanosMulCountEvery = BigInteger.valueOf(countEvery).multiply(BigInteger.valueOf(NANOS_IN_SECOND));

            while (true) {
                int workCount = 0;

                workCount = queue.read((msgTypeId, buffer1, index, length) -> {
                    long value = buffer1.getLong(index);
                    total.add(value);
                    if ((counter.getValue() & countEveryMask) == countEvery - 1) {
                        long end = System.nanoTime();
                        log(nanosMulCountEvery.divide(BigInteger.valueOf(end - startTime.getValue())) + " msgs/s");
                        startTime.setValue(end);
                    }
                    counter.incrementAndGet();
                }, 1);

                if (workCount == 0) {
                    Thread.yield();
                }
            }
        };
        Thread processor = new Thread(runnable, "processor");
        processor.start();

        AtomicBuffer bb = new UnsafeBuffer(ByteBuffer.allocateDirect(8));
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            while (!queue.write(1, bb, 0, 8)) {
                LockSupport.parkNanos(1);
            }
        }
    }

    private static void log(String msg) {
        System.out.println(Thread.currentThread().getName() + " | " + msg);
    }

    private static void trace(String msg) {
//        System.out.println(Thread.currentThread().getName() + " | " + msg);
    }
}

