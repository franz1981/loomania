package io.github.loomania;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public final class AsyncBlocker<R extends AsyncBlocker.Result> {

    public static final class Try<R extends Result> implements AutoCloseable {
        private boolean awaitClose;
        private Throwable exception;
        private final R result;

        private Try(R result) {
            this.awaitClose = false;
            this.result = result;
            this.exception = null;
        }

        private void checkExpectClosed() {
            if (awaitClose) {
                throw new IllegalStateException("awaiting close to happen on this");
            }
        }

        private void expectClose() {
            assert !awaitClose;
            awaitClose = true;
        }

        public Throwable exception() {
            return exception;
        }

        public R result() {
            if (exception() != null) {
                throw new RuntimeException(exception());
            }
            return result;
        }

        @Override
        public void close() {
            if (!awaitClose) {
                return;
            }
            awaitClose = false;
            this.exception = null;
            result.close();
        }
    }

    public interface Result extends AutoCloseable {
        @Override
        void close();
    }

    public interface BlockWithTimeout<R extends Result> {
        void block(long delay, TimeUnit unit, R result) throws Throwable;

        default void block(R result) throws Throwable {
            block(-1, null, result);
        }
    }

    private static class TimedBlockingCommand<R extends Result> implements Runnable {
        private long delay;
        private TimeUnit unit;
        private volatile Thread parked;
        private final BlockWithTimeout<R> block;
        private final Try<R> tryResult;

        public TimedBlockingCommand(final R result, final BlockWithTimeout block) {
            this.parked = null;
            this.tryResult = new Try<R>(result);
            this.block = block;
        }

        @Override
        public void run() {
            try {
                try {
                    if (delay >= 0 && unit != null) {
                        block.block(delay, unit, tryResult.result);
                    } else {
                        block.block(tryResult.result);
                    }
                } catch (Throwable t) {
                    tryResult.exception = t;
                }
            } finally {
                var awake = parked;
                parked = null;
                LockSupport.unpark(awake);
            }
        }

        private void markCurrentThreadAsParked() {
            if (parked != null) {
                throw new IllegalStateException("this thread cannot be blocked already");
            }
            parked = Thread.currentThread();
        }

        private void sendBlockCommand() {
            if (!COMMANDS.offer(this)) {
                // try create a new thread for this; we don't care if is racy, really:
                // at worse we would have more threads ready to pick this
                createAndStartPoller(this);
            }
        }

        private Try<R> sendBlockCommandAndWaitUntilUnparked() {
            assert parked != null;
            sendBlockCommand();
            // let's try check this first; if lucky no park is required at all
            while (parked != null) {
                // can spurious wakeup :"(: we need to be sure that we have been unblocked for real!
                LockSupport.park();
            }
            tryResult.expectClose();
            return tryResult;
        }

        public Try<R> managedBlock() {
            tryResult.checkExpectClosed();
            markCurrentThreadAsParked();
            return sendBlockCommandAndWaitUntilUnparked();
        }

        public Try<R> managedBlock(long delay, TimeUnit unit) {
            if (delay < 0 || unit == null) {
                throw new IllegalArgumentException("delay must be greater or equal zero and unit not null");
            }
            tryResult.checkExpectClosed();
            markCurrentThreadAsParked();
            this.delay = delay;
            this.unit = unit;
            return sendBlockCommandAndWaitUntilUnparked();
        }
    }

    private static final int CORE_SIZE = 0;

    // TODO we can use both these for monitoring purposes
    private static final CopyOnWriteArrayList<Thread> EXECUTOR_SERVICES = new CopyOnWriteArrayList<>();
    private static final AtomicInteger POLLERS = new AtomicInteger(0);

    // This can be easily replaced by a SynchronousQueue which provide a similar semantic
    private static final SynchronousQueue<Runnable> COMMANDS = new SynchronousQueue<>();

    static {
        for (int i = 0; i < CORE_SIZE; i++) {
            EXECUTOR_SERVICES.add(createAndStartPoller());
        }
    }

    static int pollersAlive() {
        return POLLERS.get();
    }

    private static Thread createAndStartPoller() {
        return createAndStartPoller(null);
    }

    private static Thread createAndStartPoller(Runnable first) {
        var poller = new Thread(() -> {
            POLLERS.incrementAndGet();
            try {
                if (first != null) {
                    first.run();
                }
                for (; ; ) {
                    final Runnable cmd = COMMANDS.take();
                    cmd.run();
                }
            } catch (Throwable ignore) {
                // NOOP
            } finally {
                POLLERS.decrementAndGet();
            }
        });
        poller.start();
        return poller;
    }

    private final TimedBlockingCommand<R> command;

    private AsyncBlocker(R result, BlockWithTimeout<R> block) {
        command = new TimedBlockingCommand<R>(result, block);
    }

    public Try<R> block(long delay, TimeUnit unit) {
        return command.managedBlock(delay, unit);
    }

    public Try<R> block() {
        return command.managedBlock();
    }

    public static <R extends Result> AsyncBlocker<R> create(R result, BlockWithTimeout<R> block) {
        return new AsyncBlocker<R>(result, block);
    }
}
