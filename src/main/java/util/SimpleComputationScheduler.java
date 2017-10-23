package util;

import io.reactivex.Scheduler;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.plugins.RxJavaPlugins;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class SimpleComputationScheduler extends Scheduler implements Executor {
    private final int threadCount;
    //    private final Scheduler scheduledTasksExecutor;
    private final SingleThreadExecutor[] executors;
    private final boolean removeTasksOnWorkerDispose;
    private int roundRobinCounter = 0;
//    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public SimpleComputationScheduler(int threadCount, ThreadFactory threadFactory, boolean removeTasksOnWorkerDispose) {
        this.threadCount = threadCount;
        this.removeTasksOnWorkerDispose = removeTasksOnWorkerDispose;
        /*this.scheduledTasksExecutor = RxJavaPlugins.createSingleScheduler(r -> {
            Thread t = new Thread(r, "betbull-computation-scheduler");
            t.setDaemon(true);
            return t;
        });*//*new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "computation-scheduler");
            t.setDaemon(true);
            return t;
        })*/


        executors = new SingleThreadExecutor[threadCount];
        for (int i = 0; i < threadCount; i++) {
            executors[i] = SingleThreadExecutor.createWithHighResolutionScheduledQueue(threadFactory, true);
        }
    }

    public SimpleComputationScheduler(int threadCount, ThreadFactory threadFactory, boolean removeTasksOnWorkerDispose, int acceptUpToSecondsAhead, int resolutionMillis, int slotCapacity) {
        this.threadCount = threadCount;
        this.removeTasksOnWorkerDispose = removeTasksOnWorkerDispose;
        /*this.scheduledTasksExecutor = RxJavaPlugins.createSingleScheduler(r -> {
            Thread t = new Thread(r, "betbull-computation-scheduler");
            t.setDaemon(true);
            return t;
        });*//*new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "computation-scheduler");
            t.setDaemon(true);
            return t;
        })*/


        executors = new SingleThreadExecutor[threadCount];
        for (int i = 0; i < threadCount; i++) {
            executors[i] = SingleThreadExecutor.createWithFastScheduler(threadFactory, acceptUpToSecondsAhead, resolutionMillis, slotCapacity);
        }
    }

    private SingleThreadExecutor pick() {
        if (threadCount == 1)
            return executors[0];

//        int i = roundRobinCounter.incrementAndGet();
        // we don't need strict sequence, so using non-volatile values is ok
        // because it reduces contention dramatically compared to using Atomics even xadd
        int i = roundRobinCounter++;

        return executors[Math.abs(i % threadCount)];
    }

    @Override
    public Worker createWorker() {
        if (removeTasksOnWorkerDispose)
            return new SingleThreadWorker(pick());
        else
            return new SingleThreadWorkerTasksUntracked(pick());
    }

    @NonNull
    @Override
    public Disposable scheduleDirect(@NonNull Runnable run) {
        SingleThreadExecutor executor = pick();

        try {
            final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);
            return executor.executeDisposable(decoratedRun);
        } catch (RejectedExecutionException ex) {
            RxJavaPlugins.onError(ex);
            return EmptyDisposable.INSTANCE;
        }
    }

    public void scheduleDirectNonDisposable(@NonNull Runnable run) {
        SingleThreadExecutor executor = pick();

        try {
            executor.execute(run);
        } catch (RejectedExecutionException ex) {
            RxJavaPlugins.onError(ex);
        }
    }

    @NonNull
    @Override
    public Disposable scheduleDirect(@NonNull Runnable run, final long delay, final TimeUnit unit) {
        final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

        return pick().schedule(decoratedRun, delay, unit);
    }

    @Override
    public void execute(Runnable command) {
        pick().execute(command);
    }

    private static class SingleThreadWorker extends Worker {
        private final ListCompositeDisposable serial;
        private final CompositeDisposable timed;
        private final ListCompositeDisposable both;

        private final SingleThreadExecutor executor;
        volatile boolean disposed;

        SingleThreadWorker(SingleThreadExecutor executor) {
            this.executor = executor;
            this.serial = new ListCompositeDisposable();
            this.timed = new CompositeDisposable();
            this.both = new ListCompositeDisposable();
            this.both.add(serial);
            this.both.add(timed);
        }

        @Override
        public void dispose() {
            if (!disposed) {
                disposed = true;
                both.dispose();
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }

        @Override
        public Disposable schedule(Runnable run) {
            if (disposed) {
                return EmptyDisposable.INSTANCE;
            }

            DisposableContainer parent = serial;

            final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

            RemoveFromParentOnDisposeRunnable sr = new RemoveFromParentOnDisposeRunnable(decoratedRun, parent);

            if (parent != null) {
                if (!parent.add(sr)) {
                    return sr;
                }
            }

            try {
                Disposable disposable = executor.executeDisposable(sr);
                sr.setDisposable(disposable);
            } catch (RejectedExecutionException ex) {
                if (parent != null) {
                    parent.remove(sr);
                }
                RxJavaPlugins.onError(ex);
            }

            return sr;
        }

        @NonNull
        @Override
        public Disposable schedule(@NonNull Runnable run, long delay, @NonNull TimeUnit unit) {
            if (delay <= 0) {
                return schedule(run);
            }
            if (disposed) {
                return EmptyDisposable.INSTANCE;
            }

            CompositeDisposable parent = timed;

            final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

            final RemoveFromParentOnDisposeRunnable sr = new RemoveFromParentOnDisposeRunnable(decoratedRun, parent);

            if (parent != null) {
                if (!parent.add(sr)) {
                    return sr;
                }
            }

            try {
                final Disposable d = executor.schedule(sr, delay, unit);
                sr.setDisposable(d);
            } catch (RejectedExecutionException ex) {
                if (parent != null) {
                    parent.remove(sr);
                }
                RxJavaPlugins.onError(ex);
            }

            return sr;
        }

        static final class RemoveFromParentOnDisposeRunnable extends AtomicReferenceArray<Object>
                implements Runnable, Disposable {

            static final Object DISPOSED = new Object();
            static final Object DONE = new Object();
            static final int PARENT_INDEX = 0;
            static final int DISPOSABLE_INDEX = 1;
            private static final long serialVersionUID = -6120223772001106981L;
            final Runnable actual;

            /**
             * Creates a ScheduledRunnable by wrapping the given action and setting
             * up the optional parent.
             *
             * @param actual the runnable to wrap, not-null (not verified)
             * @param parent the parent tracking container or null if none
             */
            public RemoveFromParentOnDisposeRunnable(Runnable actual, DisposableContainer parent) {
                super(2);
                this.actual = actual;
                this.lazySet(0, parent);
            }


            @Override
            public void run() {
                try {
                    try {
                        actual.run();
                    } catch (Throwable e) {
                        // Exceptions.throwIfFatal(e); nowhere to go
                        RxJavaPlugins.onError(e);
                    }
                } finally {
                    Object o = get(PARENT_INDEX);
                    if (o != DISPOSED && o != null && compareAndSet(PARENT_INDEX, o, DONE)) {
                        ((DisposableContainer) o).delete(this);
                    }

                    for (; ; ) {
                        o = get(DISPOSABLE_INDEX);
                        if (o == DISPOSED || compareAndSet(DISPOSABLE_INDEX, o, DONE)) {
                            break;
                        }
                    }
                }
            }

            public void setDisposable(Disposable f) {
                for (; ; ) {
                    Object o = get(DISPOSABLE_INDEX);
                    if (o == DONE) {
                        return;
                    }
                    if (o == DISPOSED) {
                        f.dispose();
                        return;
                    }
                    if (compareAndSet(DISPOSABLE_INDEX, o, f)) {
                        return;
                    }
                }
            }

            @Override
            public void dispose() {
                for (; ; ) {
                    Object o = get(DISPOSABLE_INDEX);
                    if (o == DONE || o == DISPOSED) {
                        break;
                    }
                    if (compareAndSet(DISPOSABLE_INDEX, o, DISPOSED)) {
                        if (o != null) {
                            ((Disposable) o).dispose();
                        }
                        break;
                    }
                }

                for (; ; ) {
                    Object o = get(PARENT_INDEX);
                    if (o == DONE || o == DISPOSED || o == null) {
                        return;
                    }
                    if (compareAndSet(PARENT_INDEX, o, DISPOSED)) {
                        ((DisposableContainer) o).delete(this);
                        return;
                    }
                }
            }

            @Override
            public boolean isDisposed() {
                Object o = get(DISPOSABLE_INDEX);
                return o == DISPOSED || o == DONE;
            }
        }
    }

    private static class SingleThreadWorkerTasksUntracked extends Worker {
        private final SingleThreadExecutor executor;
        volatile boolean disposed;

        SingleThreadWorkerTasksUntracked(SingleThreadExecutor executor) {
            this.executor = executor;
        }

        @Override
        public void dispose() {
            if (!disposed) {
                disposed = true;
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }

        @Override
        public Disposable schedule(Runnable run) {
            if (disposed) {
                return EmptyDisposable.INSTANCE;
            }

            final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

            try {
                Disposable disposable = executor.executeDisposable(decoratedRun);
                return disposable;
            } catch (RejectedExecutionException ex) {
                RxJavaPlugins.onError(ex);
                return Disposables.empty();
            }

        }

        @NonNull
        @Override
        public Disposable schedule(@NonNull Runnable run, long delay, @NonNull TimeUnit unit) {
            if (delay <= 0) {
                return schedule(run);
            }
            if (disposed) {
                return EmptyDisposable.INSTANCE;
            }

            final Runnable decoratedRun = RxJavaPlugins.onSchedule(run);

            try {
                return executor.schedule(decoratedRun, delay, unit);
            } catch (RejectedExecutionException ex) {
                RxJavaPlugins.onError(ex);
                return Disposables.empty();
            }
        }
    }
}
