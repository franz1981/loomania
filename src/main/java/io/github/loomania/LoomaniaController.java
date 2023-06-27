package io.github.loomania;

import java.lang.invoke.*;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;

public final class LoomaniaController {

    private static final boolean ok;
    private static final MethodHandle currentCarrierThread;
    private static final MethodHandle virtualThreadFactory;
    private static final MethodHandle threadStartWithContainer;

    static {
        boolean isOk = false;
        MethodHandle ct = null;
        MethodHandle vtf = null;
        MethodHandle tswc = null;
        try {
            MethodHandles.Lookup thr = MethodHandles.privateLookupIn(Thread.class, MethodHandles.lookup());
            ct = thr.findStatic(Thread.class, "currentCarrierThread", MethodType.methodType(Thread.class));
            Class<?> vtbClass = Class.forName("java.lang.ThreadBuilders$VirtualThreadBuilder", false, null);
            vtf = thr.findConstructor(vtbClass, MethodType.methodType(void.class, Executor.class));
            // create efficient transformer
            vtf = vtf.asType(MethodType.methodType(Thread.Builder.OfVirtual.class, Executor.class));
            //void start(jdk.internal.vm.ThreadContainer container)
            tswc = thr.findVirtual(Thread.class, "start", MethodType.methodType(void.class, jdk.internal.vm.ThreadContainer.class));
            isOk = true;
        } catch (Exception | Error e) {
            // no good
            System.err.println("Failed to initialize Loomania (" + Nope.nopeMsg() + "): " + e);
        }
        ok = isOk;
        currentCarrierThread = ct;
        virtualThreadFactory = vtf;
        threadStartWithContainer = tswc;
    }

    public static boolean isInstalled() {
        return ok;
    }

    public static boolean isVirtual(Thread thread) {
        if (! ok) throw Nope.nope();
        return thread != null && thread.isVirtual();
    }

    public static Thread currentCarrierThread() {
        if (! ok) throw Nope.nope();
        try {
            Thread currentThread = currentThread();
            return currentThread.isVirtual() ? (Thread) currentCarrierThread.invokeExact() : currentThread;
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    public static JdkVirtualThreadExecutorBuilder newJdkVirtualThreadExecutorBuilder() {
        if (! ok) throw Nope.nope();
        return new JdkVirtualThreadExecutorBuilder();
    }

    static ExecutorService buildVirtualThreadFactory(JdkVirtualThreadExecutorBuilder builder) {
        String name = builder.getName();
        int corePoolSize = max(0, builder.getCorePoolSize());
        int maximumPoolSize = min(JdkVirtualThreadExecutorBuilder.DEFAULT_MAX, min(corePoolSize, builder.getMaximumPoolSize()));
        int parallelism = min(maximumPoolSize, max(0, builder.getParallelism()));
        int minimumRunnable = min(JdkVirtualThreadExecutorBuilder.DEFAULT_MAX, max(0, builder.getMinimumRunnable()));
        Duration keepAliveTime = builder.getKeepAliveTime();
        long keepAliveMillis = keepAliveTime.toMillis();
        ForkJoinPool fjp = new ForkJoinPool(
            parallelism,
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null,
            true,
            corePoolSize,
            maximumPoolSize,
            minimumRunnable,
            pool -> true,
            keepAliveMillis,
            TimeUnit.MILLISECONDS
        );
        return newVirtualThreadExecutor(fjp, name, ExecutorServiceListener.EMPTY);
    }

    public static ExecutorService newEventLoopExecutorService(ThreadFactory carrierThreadFactory, Runnable wakeup, Consumer<? super VirtualEventLoopContext> eventLoopBody, ExecutorServiceListener listener) {
        Objects.requireNonNull(carrierThreadFactory, "carrierThreadFactory");
        Objects.requireNonNull(eventLoopBody, "eventLoopBody");
        Objects.requireNonNull(eventLoopBody, "wakeup");
        Objects.requireNonNull(listener, "listener");
        EventLoopExecutorService eventLoopExecutor = new EventLoopExecutorService(carrierThreadFactory, eventLoopBody, wakeup);
        VirtualThreadExecutorService virtualThreadExecutor = (VirtualThreadExecutorService) newVirtualThreadExecutor(eventLoopExecutor, "event loop", listener);
        eventLoopExecutor.setVirtualThreadExecutor(virtualThreadExecutor);
        return virtualThreadExecutor;
    }

    public static ExecutorService newVirtualThreadExecutor(ExecutorService delegate, String name, ExecutorServiceListener listener) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(listener, "listener");
        final ThreadFactory factory;
        Thread.Builder.OfVirtual ov = newVirtualThreadFactory(delegate);
        ov.name(name);
        factory = ov.factory();
        return new VirtualThreadExecutorService(factory, delegate, listener);
    }

    private static Thread.Builder.OfVirtual newVirtualThreadFactory(Executor executor) {
        try {
            return (Thread.Builder.OfVirtual) virtualThreadFactory.invokeExact(executor);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private LoomaniaController() {
    }

    private static void startThreadWithContainer(final Thread thread, final jdk.internal.vm.ThreadContainer threadContainer) {
        try {
            threadStartWithContainer.invokeExact(thread, threadContainer);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    /**
     * An "outer" executor service which can handle shutdown cleanly by waiting for all virtual threads to exit
     * and then shutting down a delegate service.
     * This class implements the thread container needed to track running virtual threads.
     */
    static class VirtualThreadExecutorService extends AbstractExecutorService {
        private static final VarHandle stateHandle = ConstantBootstraps.fieldVarHandle(MethodHandles.lookup(), "state", VarHandle.class, VirtualThreadExecutorService.class, long.class);

        private static final long STATE_PRE_TERMINATE_REQUESTED = 1L << 62;
        private static final long STATE_TERMINATE_REQUESTED = 1L << 63;
        private static final long STATE_NTHREADS_MASK = 0xffff_ffffL;

        final ThreadFactory virtualThreadFactory;
        final ExecutorService delegateService;
        final ExecutorServiceListener listener;

        @SuppressWarnings("unused") // stateHandle
        private volatile long state;

        private final jdk.internal.vm.ThreadContainer threadContainer = new jdk.internal.vm.ThreadContainer(false) {
            public long threadCount() {
                return state & STATE_NTHREADS_MASK;
            }

            public void onStart(final Thread thread) {
                if (! tryStartThread()) {
                    throw new RejectedExecutionException("Already shut down");
                }
            }

            public void onExit(final Thread thread) {
                exitThread();
            }

            public Stream<Thread> threads() {
                // we refuse to cooperate with this tyranny
                return Stream.of();
            }
        };

        VirtualThreadExecutorService(final ThreadFactory virtualThreadFactory, final ExecutorService delegateService, final ExecutorServiceListener listener) {
            this.virtualThreadFactory = virtualThreadFactory;
            this.delegateService = delegateService;
            this.listener = listener;
        }

        private boolean tryStartThread() {
            long oldState, newState, witness;
            oldState = state;
            for (;;) {
                if (isTerminateRequestedState(oldState)) {
                    return false;
                }
                if ((oldState & STATE_NTHREADS_MASK) == STATE_NTHREADS_MASK) {
                    // too many threads! not likely
                    return false;
                }
                newState = oldState + 1;
                witness = compareAndExchangeState(oldState, newState);
                if (witness == oldState) {
                    // success
                    return true;
                }
                // missed the CAX, try again
                oldState = witness;
            }
        }

        private void exitThread() {
            long oldState, newState, witness;
            oldState = state;
            for (;;) {
                newState = oldState - 1;
                witness = compareAndExchangeState(oldState, newState);
                if (witness == oldState) {
                    // success
                    break;
                }
                // missed the CAX, try again
                oldState = witness;
            }
            if ((oldState & STATE_NTHREADS_MASK) == 1 && isTerminateRequestedState(oldState)) {
                // the last counted thread is exiting
                emptied();
            }
        }

        public void shutdown() {
            long oldState = state;
            if (isPreTerminateRequestedState(oldState)) {
                return;
            }
            long newState;
            long witness;
            for (;;) {
                newState = oldState | STATE_PRE_TERMINATE_REQUESTED;
                witness = compareAndExchangeState(oldState, newState);
                if (witness == oldState) {
                    // shutdown has been initiated
                    break;
                }
                oldState = witness;
            }
            // now call pre-terminate task
            safeRun(listener::shutdownRequested);
            // now, update state for termination request
            // assert witness == oldState
            for (;;) {
                newState = oldState | STATE_TERMINATE_REQUESTED;
                witness = compareAndExchangeState(oldState, newState);
                if (witness == oldState) {
                    // shutdown has been initiated
                    break;
                }
                oldState = witness;
            }
            safeRun(listener::shutdownInitiated);
            if (threadCountOf(oldState) == 0) {
                // no threads are live so proceed directly to termination
                emptied();
            }
        }

        void emptied() {
            delegateService.execute(delegateService::shutdown);
            safeRun(listener::terminated);
        }

        public List<Runnable> shutdownNow() {
            shutdown();
            return List.of();
        }

        public boolean isShutdown() {
            return isPreTerminateRequestedState(state);
        }

        public boolean isTerminated() {
            return isShutdown() && delegateService.isTerminated();
        }

        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            return delegateService.awaitTermination(timeout, unit);
        }

        public void execute(final Runnable command) {
            startThreadWithContainer(virtualThreadFactory.newThread(command), threadContainer);
        }

        private long compareAndExchangeState(final long oldState, final long newState) {
            return (long) stateHandle.compareAndExchange(this, oldState, newState);
        }

        private static boolean isTerminateRequestedState(final long state) {
            return (state & STATE_TERMINATE_REQUESTED) != 0;
        }

        private static boolean isPreTerminateRequestedState(final long state) {
            return (state & STATE_PRE_TERMINATE_REQUESTED) != 0;
        }

        private static long threadCountOf(final long state) {
            return state & STATE_NTHREADS_MASK;
        }

    }

    static class EventLoopExecutorService extends AbstractExecutorService implements ExecutorServiceListener, VirtualEventLoopContext {
        private final ConcurrentLinkedQueue<Runnable> sharedQueue = new ConcurrentLinkedQueue<>();
        private final ArrayDeque<Runnable> localQueue = new ArrayDeque<>(1024);
        private final CountDownLatch terminationLatch = new CountDownLatch(1);
        // ArrayDeque::add always returns {@code true}, which is very convenient...
        private final Predicate<Runnable> bulkRemover;
        private final Thread carrierThread;
        private final Runnable wakeup;
        private final Consumer<? super VirtualEventLoopContext> eventLoopBody;

        // internal carrier-thread-local state
        private VirtualThreadExecutorService virtualThreadExecutor;

        private Thread eventLoopThread;
        private volatile Runnable eventLoopThreadContinuation;
        private boolean continueEventLoop;

        EventLoopExecutorService(ThreadFactory carrierThreadFactory, Consumer<? super VirtualEventLoopContext> eventLoopBody, Runnable wakeup) {
            bulkRemover = this::moveFromSharedToLocalTasks;
            carrierThread = carrierThreadFactory.newThread(this::carrierThreadBody);
            carrierThread.start();
            this.eventLoopBody = eventLoopBody;
            this.wakeup = wakeup;
        }

        private boolean moveFromSharedToLocalTasks(Runnable task) {
            Objects.requireNonNull(task);
            if (task == eventLoopThreadContinuation && !continueEventLoop) {
                continueEventLoop = true;
            } else {
                localQueue.add(task);
            }
            return true;
        }

        public void shutdown() {
            // we can safely assume that all virtual threads must be gone, and that this is called exactly once from the carrier
            terminationLatch.countDown();
        }

        public List<Runnable> shutdownNow() {
            shutdown();
            return List.of();
        }

        public boolean isShutdown() {
            throw new UnsupportedOperationException();
        }

        public boolean isTerminated() {
            return terminationLatch.getCount() == 0;
        }

        public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
            return terminationLatch.await(timeout, unit);
        }

        private boolean handleFirstTimeEventLoopVThread(Runnable command) {
            if (eventLoopThreadContinuation != null) {
                return false;
            }
            // let continueEventLoop to piggyback on the seq-cst store of eventLoopThreadContinuation:
            // by consequence, we need to load-acquire eventLoopThreadContinuation before reading continueEventLoop
            continueEventLoop = true;
            eventLoopThreadContinuation = command;
            // just in case the carrier thread already parked, wake it up
            LockSupport.unpark(carrierThread);
            return true;
        }

        public void execute(final Runnable command) {
            // this will happen just once
            if (handleFirstTimeEventLoopVThread(command)) {
                return;
            }
            // common path
            if (currentCarrierThread() == carrierThread) {
                if (eventLoopThreadContinuation == command) {
                    // takes priority
                    continueEventLoop = true;
                } else {
                    localQueue.add(command);
                }
            } else {
                sharedQueue.add(command);
                try {
                    wakeup.run();
                } catch (Throwable ignored) {}
                // the carrier might be parked if all of its threads are asleep
                LockSupport.unpark(carrierThread);
            }
        }

        void setVirtualThreadExecutor(final VirtualThreadExecutorService virtualThreadExecutor) {
            this.virtualThreadExecutor = virtualThreadExecutor;
            // capture the event loop thread before we do anything else
            eventLoopThread = virtualThreadExecutor.virtualThreadFactory.newThread(() -> eventLoopBody.accept(this));
            VarHandle.fullFence();
            // this indirectly calls {@code #execute} with the event loop's continuation, before any other call is possible
            // also, note that we are starting this thread outside of the container so that it does not stop the outer executor from terminating
            eventLoopThread.start();
        }

        private void continueEventLoop() {
            // load-acquire eventLoopThreadContinuation first: if non-null
            // the continueEventLoop value read right after is either the one set at handleFirstTimeEventLoopVThread
            // or the other ones set in the event loop carrier thread (ie single-threaded).
            var eventLoopContinuation = eventLoopThreadContinuation;
            if (eventLoopContinuation == null) {
                return;
            }
            if (!continueEventLoop) {
                return;
            }
            continueEventLoop = false;
            safeRun(eventLoopContinuation);
        }

        /**
         * The actual body of the carrier thread task.
         */
        private void carrierThreadBody() {
            try {
                for (;;) {
                    // drain shared queue
                    sharedQueue.removeIf(bulkRemover);
                    // run next task
                    continueEventLoop();
                    // try to run one more task before continuing the event loop again
                    if (safeRun(localQueue.poll()) == null) {
                        // don't explicitly unpark the event loop thread because it's either already waiting or it's doing something else
                        // wait for a new task to come in on the shared queue
                        LockSupport.park();
                    }
                }
            } finally {
                // this last bit is called from outside the virtual thread universe!
                // TODO we need it?
                // eventLoop.terminationComplete();
            }
        }

        private boolean carrierThreadHasTasks() {
            assert Thread.currentThread() == eventLoopThread;
            return !localQueue.isEmpty() || !sharedQueue.isEmpty();
        }

        @Override
        public boolean tryBlock() {
            assert Thread.currentThread() == eventLoopThread;
            if (!carrierThreadHasTasks()) {
                // TODO: set the state of the event loop virtual thread here!
                return true;
            }
            return false;
        }

        @Override
        public void yield() {
            assert Thread.currentThread() == eventLoopThread;
            // in the v thread of the event loop
            if (!carrierThreadHasTasks()) {
                return;
            }
            Thread.yield();
        }

        /**
         * The outer executor service is being requested to terminate.
         */
        public void shutdownRequested() {
            // submit the shutdown thread task to the outer executor before it starts rejecting tasks
            // TODO: TOFIX virtualThreadExecutor is NECESSARY?????
            // virtualThreadExecutor.execute(eventLoop::requestTermination);
        }

        public void shutdownInitiated() {
            System.nanoTime();
        }

        public void terminated() {
            System.nanoTime();
        }
    }

    private static Runnable safeRun(Runnable r) {
        if (r != null) try {
            r.run();
        } catch (Throwable ignored) {
            // no safe way to handle this
        }
        return r;
    }
}
