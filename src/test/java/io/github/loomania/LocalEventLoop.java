package io.github.loomania;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.Executor;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public final class LocalEventLoop implements Executor, AutoCloseable {

    public enum State {
        NOT_STARTED,
        STARTED,
        CLOSING,
        CLOSED
    }

    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private final Executor executor;
    private volatile Thread elThread;
    private final CompletableFuture<?> CLOSED = new CompletableFuture<>();
    private static final Runnable POISON_PILL = () -> {
    };
    private final Selector selector;
    private volatile boolean sleeping;

    public LocalEventLoop(Executor executor) {
        this.executor = executor;
        try {
            this.selector = Selector.open();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void execute(Runnable task) {
        var s = state.get();
        if (s != State.STARTED) {
            if (s == State.CLOSING || s == State.CLOSED) {
                throw new RejectedExecutionException();
            }
            tasks.offer(task);
            if (state.compareAndSet(State.NOT_STARTED, State.STARTED)) {
                doStartEventLoop();
            }
            wakeup();
        } else {
            tasks.offer(task);
            wakeup();
        }
    }

    private void doStartEventLoop() {
        assert state.get() == State.STARTED;
        executor.execute(this::eventLoop);
    }

    private void eventLoop() {
        assert elThread == null;
        this.elThread = Thread.currentThread();
        for (; ; ) {
            final var cmd = tasks.poll();
            if (cmd == null) {
                sleeping = true;
                try {
                    if (tasks.isEmpty()) {
                        selector.select();
                    }
                } catch (IOException e) {
                    // TODO ignore?
                } finally {
                    sleeping = false;
                    continue;
                }
            }
            if (cmd == POISON_PILL) {
                break;
            }
            safeRun(cmd);
        }
        // shutting down
        safeRunTasks();
        // closed
        State s = state.getAndSet(State.CLOSED);
        CLOSED.complete(null);
        assert s == State.CLOSING;
    }

    private static void safeRun(Runnable cmd) {
        try {
            cmd.run();
        } catch (Throwable ignore) {

        }
    }

    private void safeRunTasks() {
        Runnable cmd;
        while ((cmd = tasks.poll()) != null) {
            safeRun(cmd);
        }
    }

    @Override
    public void close() {
        for (;;) {
            var s = state.get();
            switch (s) {

                case NOT_STARTED -> {
                    if (state.compareAndSet(State.NOT_STARTED, State.CLOSED)) {
                        CLOSED.complete(null);
                        return;
                    };
                }
                case STARTED -> {
                    if (state.compareAndSet(State.STARTED, State.CLOSING)) {
                        tasks.add(POISON_PILL);
                        wakeup();
                        if (!inEventLoop()) {
                            CLOSED.join();
                        }
                        return;
                    }
                }
                case CLOSING, CLOSED -> {
                    if (!inEventLoop()) {
                        CLOSED.join();
                    }
                    return;
                }
            }
        }
    }

    private boolean inEventLoop() {
        return elThread == Thread.currentThread();
    }

    private void wakeup() {
        if (inEventLoop()) {
            return;
        }
        if (sleeping) {
            selector.wakeup();
        }
    }
}