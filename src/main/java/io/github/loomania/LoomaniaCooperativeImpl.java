package io.github.loomania;

import io.github.loomania.LoomaniaImpl.VirtualThreadExecutorService;

import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.github.loomania.LoomaniaImpl.currentCarrierThread;
import static io.github.loomania.LoomaniaImpl.newVirtualThreadExecutor;

final class LoomaniaCooperativeImpl {

    public static ExecutorService newEventLoopExecutorService(ScopedValue_Temporary.Carrier carrier, Runnable wakeup, Consumer<? super VirtualEventLoopContext> eventLoopBody, ExecutorServiceListener listener) {
        Objects.requireNonNull(eventLoopBody, "eventLoopBody");
        Objects.requireNonNull(wakeup, "wakeup");
        Objects.requireNonNull(listener, "listener");
        EventLoopExecutorService eventLoopExecutor = new EventLoopExecutorService(eventLoopBody, wakeup);
        VirtualThreadExecutorService virtualThreadExecutor = (VirtualThreadExecutorService) newVirtualThreadExecutor(carrier, eventLoopExecutor, "event loop", listener);
        eventLoopExecutor.setVirtualThreadExecutor(virtualThreadExecutor);
        return virtualThreadExecutor;
    }
    private LoomaniaCooperativeImpl() {
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

        EventLoopExecutorService(Consumer<? super VirtualEventLoopContext> eventLoopBody, Runnable wakeup) {
            bulkRemover = this::moveFromSharedToLocalTasks;
            carrierThread = new Thread(this::carrierThreadBody, "Event loop thread");
            carrierThread.setDaemon(true);
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
