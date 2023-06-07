package io.github.loomania;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public final class VirtualLocalEventLoop implements Executor, AutoCloseable {

    public enum State {
        NOT_STARTED,
        STARTED,
        CLOSING,
        CLOSED
    }

    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
    private volatile Thread elThread;
    private final CompletableFuture<?> CLOSED = new CompletableFuture<>();
    private static final Runnable POISON_PILL = () -> {
    };
    private final Selector selector;
    private volatile boolean sleeping = true;

    public VirtualLocalEventLoop() {
        try {
            this.selector = Selector.open();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void execute(Runnable task) {
        var s = state.get();
        if (s != State.STARTED && s != State.NOT_STARTED) {
            throw new RejectedExecutionException();
        }
        tasks.offer(task);
        wakeup();
    }

    public void eventLoop(VirtualEventLoopContext context) {
        if (!state.compareAndSet(State.NOT_STARTED, State.STARTED)) {
            // TODO
            return;
        }
        sleeping = false;
        assert elThread == null;
        this.elThread = Thread.currentThread();
        for (; ; ) {
            context.yield();
            final var cmd = tasks.poll();
            if (cmd == null) {
                sleeping = true;
                try {
                    if (tasks.isEmpty()) {
                        if (!context.tryBlock()) {
                            continue;
                        }
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

    public void wakeup() {
        if (inEventLoop()) {
            return;
        }
        if (sleeping) {
            selector.wakeup();
        }
    }
}