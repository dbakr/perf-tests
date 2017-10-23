package util;

import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;
import org.agrona.concurrent.HighResolutionTimer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.hints.ThreadHints;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class SingleThreadExecutor implements AutoCloseable, Runnable, Executor {
    // Idle strategy variables
    private static final long MaxSpins = 10;
    private static final long MaxYields = 10;
    private static final long MinParkNanos = TimeUnit.MICROSECONDS.toNanos(100);
    private static final long MaxParkNanos = TimeUnit.MILLISECONDS.toNanos(2000);

    static {
        HighResolutionTimer.enable();
    }

    // This one is MPSC (ManyToOne) concurrent queue for tasks scheduled from other threads
    private final Queue<Runnable> externallyAddedTasks;
    // This is a non-thread-safe queue to schedule tasks from within running thread
    private final Queue<Runnable> locallyAddedTasks = new ArrayDeque<>(65536);
    // This is non-thread-safe queue for delayed (scheduled) tasks
    private final DelayedWorkQueue delayedTasks;
    private final boolean removeScheduledTasksOnCancel;
    private final Thread runningThread;
    private final AtomicBoolean parking = new AtomicBoolean(false);

    private IdleState state = IdleState.NOT_IDLE;

    private long spins;
    private long yields;
    private long parkPeriodNs;

    private volatile boolean isClosed = false;

    private SingleThreadExecutor(ThreadFactory threadFactory, boolean removeScheduledTasksOnCancel) {
        this.removeScheduledTasksOnCancel = removeScheduledTasksOnCancel;
        externallyAddedTasks = new ManyToOneConcurrentArrayQueue<>(65536);
        runningThread = threadFactory.newThread(this);
        delayedTasks = new HeapDelayedWorkQueue(this);
        runningThread.start();
    }

    private SingleThreadExecutor(ThreadFactory threadFactory, int acceptUpToSecondsAhead, int resolutionMillis, int slotCapacity) {
        this.removeScheduledTasksOnCancel = false;
        externallyAddedTasks = new ManyToOneConcurrentArrayQueue<>(655436);
        runningThread = threadFactory.newThread(this);
        delayedTasks = new WaitListDelayedWorkQueue(acceptUpToSecondsAhead, resolutionMillis, slotCapacity, this);
        runningThread.start();
    }

    public static SingleThreadExecutor createWithHighResolutionScheduledQueue(ThreadFactory threadFactory, boolean removeScheduledTasksOnCancel) {
        return new SingleThreadExecutor(threadFactory, removeScheduledTasksOnCancel);
    }

    public static SingleThreadExecutor createWithFastScheduler(ThreadFactory threadFactory, int acceptUpToSecondsAhead, int resolutionMillis, int slotCapacity) {
        return new SingleThreadExecutor(threadFactory, acceptUpToSecondsAhead, resolutionMillis, slotCapacity);
    }

    private static long nowNanos() {
        return System.nanoTime();
    }

    @Override
    public void execute(Runnable command) {
        addTask(command);
    }

    boolean inRunningThread() {
        return Thread.currentThread() == runningThread;
    }

    public boolean tryExecute(Runnable command) {
        boolean succeded;
        if (inRunningThread())
            succeded = locallyAddedTasks.offer(command);
        else
            succeded = externallyAddedTasks.offer(command);

        wakeUp();
        return succeded;
    }

    private void idle(int workCount) {
        if (workCount > 0) {
            resetIdleCounters();
        } else {
            idle();
        }
    }


    private void idle() {
        switch (state) {
            case NOT_IDLE:
                state = IdleState.SPINNING;
                spins++;
                break;

            case SPINNING:
                ThreadHints.onSpinWait();
                if (++spins > MaxSpins) {
                    state = IdleState.YIELDING;
                    yields = 0;
                }
                break;

            case YIELDING:
                if (++yields > MaxYields) {
                    state = IdleState.PARKING;
                    parkPeriodNs = MinParkNanos;
                } else {
                    Thread.yield();
                }
                break;

            case PARKING:
                parking.lazySet(true);
                LockSupport.parkNanos(Math.min(parkPeriodNs, delayedTasks.timeToNextTaskNanos()));
                parking.lazySet(false);
                parkPeriodNs = Math.min(parkPeriodNs << 1, MaxParkNanos);
                break;
        }
    }

    private void resetIdleCounters() {
        spins = 0;
        yields = 0;
        state = IdleState.NOT_IDLE;
    }

    private void wakeUp() {
        if (parking.compareAndSet(true, false))
            LockSupport.unpark(runningThread);
    }

    public Disposable executeDisposable(Runnable command) {
        SimpleDisposableRunnable disposableRunnable = new SimpleDisposableRunnable(command);
        addTask(disposableRunnable);
        return disposableRunnable;
    }

    private void addTask(Runnable task) {
        if (inRunningThread()) {
            if (!locallyAddedTasks.offer(task))
                throw new RejectedExecutionException("Can't schedule task for execution, local queue is full");
        } else {
            if (!externallyAddedTasks.offer(task))
                throw new RejectedExecutionException("Can't schedule task for execution, external queue is full");
        }

        wakeUp();
    }

    public Disposable schedule(Runnable command, long delay, TimeUnit unit) {
        Disposable disposable = delayedTasks.offer(command, delay, unit);

        wakeUp();

        return disposable;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public void run() {
        final Queue<Runnable> locallyAddedTasks = this.locallyAddedTasks;
        final Queue<Runnable> externallyAddedTasks = this.externallyAddedTasks;
        final DelayedWorkQueue delayedTasks = this.delayedTasks;

        while (!isClosed) {
            int workCount = 0;
            long nanoTime = nowNanos();
            delayedTasks.preFetch(nanoTime);

            Runnable delayedTask;
            while ((delayedTask = delayedTasks.poll(nanoTime)) != null) {
                workCount++;
                if (!locallyAddedTasks.offer(delayedTask)) {
                    // todo review this approach and error handling
                    delayedTasks.reschedule(delayedTask);
                    break;
                }
            }

            Runnable task = null;
            boolean localQueueIsEmpty = locallyAddedTasks.isEmpty();
            // poll both queues, one poll from each queues each cycle
            do {
                // local queue can be offered only from local thread,
                // so if it it is fully drain during this loop, we mark it as empty
                // and continue with external queue until its empty
                if (!localQueueIsEmpty) {
                    task = locallyAddedTasks.poll();
                    if (task == null) {
                        localQueueIsEmpty = true;
                    } else {
                        workCount++;
                        runTask(task);
                    }
                }
                task = externallyAddedTasks.poll();
                if (task != null) {
                    workCount++;
                    runTask(task);
                }

            } while (task != null);

            idle(workCount);
        }
    }

    private void runTask(Runnable r) {
        try {
            r.run();
        } catch (Throwable e) {
            //todo ?
            RxJavaPlugins.onError(e);
        }
    }

    enum IdleState {
        NOT_IDLE, SPINNING, YIELDING, PARKING
    }

    interface DelayedWorkQueue {
        Disposable offer(Runnable command, long delay, TimeUnit unit);

        Runnable poll(long nanoTime);

        void preFetch(long nanoTime);

        long timeToNextTaskNanos();

        void reschedule(Runnable delayedTask);
    }

    private static class SimpleDisposableRunnable extends AtomicBoolean implements Runnable, Disposable {

        final Runnable actual;

        SimpleDisposableRunnable(Runnable actual) {
            this.actual = actual;
        }

        @Override
        public void run() {
            // don't run if cancelled
            if (get()) {
                return;
            }
            try {
                actual.run();
            } finally {
                lazySet(true);
            }
        }

        @Override
        public void dispose() {
            lazySet(true);
        }

        @Override
        public boolean isDisposed() {
            return get();
        }
    }

    private static class WaitListDelayedWorkQueue implements DelayedWorkQueue {
        private final long resolutionNanos;
        // we can schedule only for specified number seconds ahead
        private final List<ArrayDeque<Runnable>> waitLists;
        // precalculated variables
        private final long acceptUpToSecondsAheadInNanos;
        private final SingleThreadExecutor owner;
        private final int slotCount;

        private final ManyToOneConcurrentLinkedQueue<ScheduledTask> toScheduleQueue;
        // currentlty processed slot index
        private int zeroTimeIndex = 0;
        // currently processed time index
        private long zeroTimeNanos;
        private long scheduledTasksCount = 0;

        private WaitListDelayedWorkQueue(int acceptUpToSecondsAhead, int resolutionMillis, int slotCapacity, SingleThreadExecutor owner) {
            acceptUpToSecondsAheadInNanos = ((long) acceptUpToSecondsAhead) * 1000L * 1000L * 1000L;
            this.owner = owner;

            long resolutionNanos = ((long) resolutionMillis) * 1000L * 1000L;

            if (resolutionNanos > acceptUpToSecondsAheadInNanos)
                throw new IllegalArgumentException("resolutionMillis should be less than acceptUpToSecondsAhead in milliseconds");

            if ((acceptUpToSecondsAheadInNanos / resolutionNanos) * resolutionNanos != acceptUpToSecondsAheadInNanos)
                throw new IllegalArgumentException("resolutionMillis should evenly divide a second to equal parts.");

            this.resolutionNanos = resolutionNanos;
            slotCount = (int) (acceptUpToSecondsAheadInNanos / resolutionNanos);
            this.waitLists = new ArrayList<>(slotCount);

            for (int i = 0; i < slotCount; i++) {
                waitLists.add(i, new ArrayDeque<>(slotCapacity));
            }
            zeroTimeNanos = nowNanos();

            toScheduleQueue = new ManyToOneConcurrentLinkedQueue<>();
        }

        @Override
        public Disposable offer(Runnable command, long delay, TimeUnit unit) {
            if (delay < 0) {
                throw new IllegalArgumentException("delay should be > 0");
            }

            final long plannedTimeNanos = nowNanos() + unit.toNanos(delay);
//            final SimpleDisposableRunnable disposableRunnable = new SimpleDisposableRunnable(command);
            final ScheduledTask scheduledTask = new ScheduledTask(command, plannedTimeNanos);

            // if we are in the running thread, omit intermediate scheduling queue and schedule directly
            if (owner.inRunningThread())
                doScheduleTask(scheduledTask);
            else
                toScheduleQueue.offer(scheduledTask);

            return scheduledTask;
        }

        @Override
        public Runnable poll(long nanoTime) {
            Runnable runnable = waitLists.get(zeroTimeIndex).poll();
            if (runnable != null) {
                scheduledTasksCount--;
            }
            return runnable;
        }

        @Override
        public void preFetch(long nanoTime) {
            // skip to current time's index skipping empty slots
            while (((nanoTime - zeroTimeNanos) > resolutionNanos) && waitLists.get(zeroTimeIndex).isEmpty()) {
                zeroTimeNanos += resolutionNanos;
                zeroTimeIndex = (zeroTimeIndex + 1) % slotCount;
            }

            // now we schedule tasks from schedule queue

            ScheduledTask scheduledTask;
            while ((scheduledTask = toScheduleQueue.poll()) != null) {
                doScheduleTask(scheduledTask);
            }
        }

        @Override
        public long timeToNextTaskNanos() {
            if (scheduledTasksCount == 0)
                return Long.MAX_VALUE;

            for (int i = 0; i < slotCount; i++) {
                if (!waitLists.get((zeroTimeIndex + i) % slotCount).isEmpty())
                    return zeroTimeNanos + (resolutionNanos * i) - nowNanos();
            }
            // we never come here but compiler doesn't know
            return Long.MAX_VALUE;
        }

        @Override
        public void reschedule(Runnable delayedTask) {
            doScheduleTask((ScheduledTask) delayedTask);
        }

        private boolean doScheduleTask(ScheduledTask scheduledTask) {
            // check if was disposed in the meantime
            if (scheduledTask.isDisposed())
                return false;

            final long plannedTimeNanos = scheduledTask.plannedTimeNanos;
            long zeroTimeNanos = this.zeroTimeNanos;
            if (plannedTimeNanos > zeroTimeNanos + acceptUpToSecondsAheadInNanos) {
                // this means working runningThread is not able to process all planned tasks, delayed and immediate
                // too bad, we can't easily signal to calling runningThread that delayed task scheduling was unsuccessful
                //todo think about unsuccessful scheduling issue: should we expand wait list? should we put it in latest one?
                RxJavaPlugins.onError(new RejectedExecutionException("Can't schedule the task, processing is too slow or too distant future specified: " + (plannedTimeNanos - zeroTimeNanos)));
            }

            int queueIndex = (zeroTimeIndex + (int) ((plannedTimeNanos - zeroTimeNanos) / resolutionNanos)) % slotCount;

            if (!waitLists.get(queueIndex).offer(scheduledTask)) {
                RxJavaPlugins.onError(new RejectedExecutionException("Can't schedule the task, corresponding time slot is full"));
                return false;
            } else {
                scheduledTasksCount++;
                return true;
            }
        }

        private static class ScheduledTask extends SimpleDisposableRunnable {
            private final long plannedTimeNanos;

            ScheduledTask(Runnable actual, long plannedTimeNanos) {
                super(actual);

                this.plannedTimeNanos = plannedTimeNanos;
            }
        }
    }

    /**
     * This is an adopted version of a heap from JSE's ScheduledThreadPoolExecutor
     */
    private static class HeapDelayedWorkQueue implements DelayedWorkQueue {

        /*
         * A DelayedWorkQueue is based on a heap-based data structure
         * like those in DelayQueue and PriorityQueue, except that
         * every ScheduledFutureTask also records its index into the
         * heap array. This eliminates the need to find a task upon
         * cancellation, greatly speeding up removal (down from O(n)
         * to O(log n)), and reducing garbage retention that would
         * otherwise occur by waiting for the element to rise to top
         * before clearing. But because the queue may also hold
         * RunnableScheduledFutures that are not ScheduledFutureTasks,
         * we are not guaranteed to have such indices available, in
         * which case we fall back to linear search. (We expect that
         * most tasks will not be decorated, and that the faster cases
         * will be much more common.)
         *
         * All heap operations must record index changes -- mainly
         * within siftUp and siftDown. Upon removal, a task's
         * heapIndex is set to -1. Note that ScheduledFutureTasks can
         * appear at most once in the queue (this need not be true for
         * other kinds of tasks or work queues), so are uniquely
         * identified by heapIndex.
         */

        private static final int INITIAL_CAPACITY = 10240;
        //        private final boolean removeScheduledTasksOnCancel;
//      private final Thread runningThread;
        private final ManyToOneConcurrentLinkedQueue<ScheduledFutureTask> toScheduleQueue;
        private final ManyToOneConcurrentLinkedQueue<ScheduledFutureTask> toRemoveQueue;
        private final SingleThreadExecutor owner;
        /**
         * Condition signalled when a newer task becomes available at the
         * head of the queue or a new runningThread may need to become leader.
         */
        private ScheduledFutureTask[] queue = new ScheduledFutureTask[INITIAL_CAPACITY];
        private int size = 0;

        private HeapDelayedWorkQueue(SingleThreadExecutor owner/*boolean removeScheduledTasksOnCancel, Thread runningThread*/) {
            this.owner = owner;
//            this.removeScheduledTasksOnCancel = removeScheduledTasksOnCancel;
//            this.runningThread = runningThread;
            toScheduleQueue = new ManyToOneConcurrentLinkedQueue<>();
            toRemoveQueue = new ManyToOneConcurrentLinkedQueue<>();
        }

        /**
         * Sets f's heapIndex if it is a ScheduledFutureTask.
         */
        private void setIndex(ScheduledFutureTask f, int idx) {
            f.heapIndex = idx;
        }

        /**
         * Sifts element added at bottom up to its heap-ordered spot.
         * Call only when holding lock.
         */
        private void siftUp(int k, ScheduledFutureTask key) {
            while (k > 0) {
                int parent = (k - 1) >>> 1;
                ScheduledFutureTask e = queue[parent];
                if (key.compareTo(e) >= 0)
                    break;
                queue[k] = e;
                setIndex(e, k);
                k = parent;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /**
         * Sifts element added at top down to its heap-ordered spot.
         * Call only when holding lock.
         */
        private void siftDown(int k, ScheduledFutureTask key) {
            int half = size >>> 1;
            while (k < half) {
                int child = (k << 1) + 1;
                ScheduledFutureTask c = queue[child];
                int right = child + 1;
                if (right < size && c.compareTo(queue[right]) > 0)
                    c = queue[child = right];
                if (key.compareTo(c) <= 0)
                    break;
                queue[k] = c;
                setIndex(c, k);
                k = child;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /**
         * Resizes the heap array.  Call only when holding lock.
         */
        private void grow() {
            int oldCapacity = queue.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1); // grow 50%
            if (newCapacity < 0) // overflow
                newCapacity = Integer.MAX_VALUE;
            queue = Arrays.copyOf(queue, newCapacity);
        }

        private boolean remove(ScheduledFutureTask x) {
            int i = x.heapIndex;
            if (i < 0)
                return false;

            setIndex(queue[i], -1);
            int s = --size;
            ScheduledFutureTask replacement = queue[s];
            queue[s] = null;
            if (s != i) {
                siftDown(i, replacement);
                if (queue[i] == replacement)
                    siftUp(i, replacement);
            }
            return true;
        }

        ScheduledFutureTask peek() {
            return queue[0];
        }

        private void doScheduleTask(ScheduledFutureTask scheduledTask) {
            if (scheduledTask.isDisposed())
                return;

            int i = size;
            if (i >= queue.length)
                grow();
            size = i + 1;
            if (i == 0) {
                queue[0] = scheduledTask;
                setIndex(scheduledTask, 0);
            } else {
                siftUp(i, scheduledTask);
            }
        }

        @Override
        public Disposable offer(Runnable command, long delay, TimeUnit unit) {
            if (command == null)
                throw new NullPointerException();

            final ScheduledFutureTask scheduledTask =
                    owner.removeScheduledTasksOnCancel ?
                            new RemoveOnCancelScheduledTask(command, triggerTime(delay, unit)) :
                            new ScheduledFutureTask(command, triggerTime(delay, unit));

            // if we are in the running thread, omit intermediate scheduling queue and schedule directly
            if (owner.inRunningThread())
                doScheduleTask(scheduledTask);
            else
                toScheduleQueue.offer(scheduledTask);

            return scheduledTask;
        }

        /**
         * Returns the trigger time of a delayed action.
         */
        private long triggerTime(long delay, TimeUnit unit) {
            return triggerTime(unit.toNanos((delay < 0) ? 0 : delay));
        }

        /**
         * Returns the trigger time of a delayed action.
         */
        private long triggerTime(long delay) {
            long now = nowNanos();
            return now + ((delay < (Long.MAX_VALUE >> 1)) ? delay : overflowFree(delay, now));
        }

        /**
         * Constrains the values of all delays in the queue to be within
         * Long.MAX_VALUE of each other, to avoid overflow in compareTo.
         * This may occur if a task is eligible to be dequeued, but has
         * not yet been, while some other task is added with a delay of
         * Long.MAX_VALUE.
         */
        private long overflowFree(long delay, long now) {
            ScheduledFutureTask head = peek();
            if (head != null) {
                long headDelay = head.time - now;
                if (headDelay < 0 && (delay - headDelay < 0))
                    delay = Long.MAX_VALUE + headDelay;
            }
            return delay;
        }

        @Override
        public Runnable poll(long nowNanos) {
            ScheduledFutureTask first = queue[0];
            if (first == null || (first.time - nowNanos) > 0)
                return null;
            else
                return finishPoll(first);
        }

        @Override
        public void preFetch(long now) {
            ScheduledFutureTask scheduledTask;

            while ((scheduledTask = toScheduleQueue.poll()) != null) {
                doScheduleTask(scheduledTask);
            }

            while ((scheduledTask = toRemoveQueue.poll()) != null) {
                remove(scheduledTask);

            }
        }

        @Override
        public long timeToNextTaskNanos() {
            ScheduledFutureTask first = queue[0];
            return first == null ? Long.MAX_VALUE : (first.time - nowNanos());
        }

        @Override
        public void reschedule(Runnable delayedTask) {
            doScheduleTask((ScheduledFutureTask) delayedTask);
        }

        /**
         * Performs common bookkeeping for poll and take: Replaces
         * first element with last and sifts it down.  Call only when
         * holding lock.
         *
         * @param f the task to remove and return
         */
        private ScheduledFutureTask finishPoll(ScheduledFutureTask f) {
            int s = --size;
            ScheduledFutureTask x = queue[s];
            queue[s] = null;
            if (s != 0)
                siftDown(0, x);
            setIndex(f, -1);
            return f;
        }


        public void clear() {
            for (int i = 0; i < size; i++) {
                ScheduledFutureTask t = queue[i];
                if (t != null) {
                    queue[i] = null;
                    setIndex(t, -1);
                }
            }
            size = 0;
        }

        private static class ScheduledFutureTask implements Disposable, Runnable {
            /**
             * Sequence number to break scheduling ties, and in turn to
             * guarantee FIFO order among tied entries.
             */
            private static final AtomicLong sequencer = new AtomicLong();

            private final Runnable runnable;

            /**
             * Sequence number to break ties FIFO
             */
            private final long sequenceNumber;
            /**
             * The time the task is enabled to execute in nanoTime units
             */
            private final long time;
            /**
             * Index into delay queue, to support faster cancellation.
             */
            int heapIndex = -1;
            private boolean cancelled = false;

            /**
             * Creates a one-shot action with given nanoTime-based trigger time.
             */
            ScheduledFutureTask(Runnable r, long ns) {
                this.runnable = r;
                this.time = ns;
                this.sequenceNumber = sequencer.getAndIncrement();
            }

            @Override
            public void dispose() {
                this.cancelled = true;
            }

            @Override
            public boolean isDisposed() {
                return cancelled;
            }

            public int compareTo(ScheduledFutureTask other) {
                if (other == this) // compare zero if same object
                    return 0;
                long diff = time - other.time;
                if (diff < 0)
                    return -1;
                else if (diff > 0)
                    return 1;
                else if (sequenceNumber < other.sequenceNumber)
                    return -1;
                else
                    return 1;
            }

            @Override
            public void run() {
                if (!cancelled)
                    runnable.run();
            }
        }

        private class RemoveOnCancelScheduledTask extends ScheduledFutureTask {

            /**
             * Creates a one-shot action with given nanoTime-based trigger time.
             *
             * @param r
             * @param ns
             */
            RemoveOnCancelScheduledTask(Runnable r, long ns) {
                super(r, ns);
            }

            @Override
            public void dispose() {
                super.dispose();
                toRemoveQueue.offer(this);
//                LockSupport.unpark(runningThread);
                owner.wakeUp();
            }
        }
    }

    private static class RunnableHolder {
        private Runnable runnable;

        public Runnable getRunnable() {
            return runnable;
        }

        public void setRunnable(Runnable runnable) {
            this.runnable = runnable;
        }
    }
}
