package io.github.loomania;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncBlockerTest {

    private static class MutableRef<T> implements AsyncBlocker.Result {

        public T ref;

        @Override
        public void close() {
            ref = null;
        }
    }

    @Test
    public void shouldFailBlockingWithoutClosingPreviousResults() throws ExecutionException, InterruptedException {
        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            var blockResult = vExecutor.submit(() -> {
                var blocker = AsyncBlocker.create(new MutableRef<Boolean>(), (delay, unit, result) -> result.ref = true);
                blocker.block();
                try {
                    blocker.block();
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            });
            Assertions.assertNotNull(blockResult.get());
        }
    }

    @Test
    public void shouldCreateNewPollerThreadsOnDemand() throws InterruptedException, BrokenBarrierException {
        int concurrency = 2;
        var allBlocked = new CyclicBarrier(concurrency + 1);
        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < concurrency; i++) {
                vExecutor.execute(() -> {
                    var blocker = AsyncBlocker.create(() -> {
                    }, (delay, unit, result) -> {
                        allBlocked.await();
                    });
                    blocker.block().close();
                });
            }
            allBlocked.await();
        }
    }

    @Test
    public void shouldRunTheBlockCommandOnNativeThread() throws ExecutionException, InterruptedException {
        var vThreadRef = new AtomicReference<Thread>();
        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            var blockResult = vExecutor.submit(() -> {
                vThreadRef.set(Thread.currentThread());
                var blocker = AsyncBlocker.create(new MutableRef<Thread>(), (delay, unit, result) -> result.ref = Thread.currentThread());
                try (var result = blocker.block()) {
                    return result.result().ref;
                }
            });
            Assertions.assertNotSame(vThreadRef.get(), blockResult.get());
            Assertions.assertTrue(vThreadRef.get().isVirtual());
            Assertions.assertFalse(blockResult.get().isVirtual());
        }
    }

    @Test
    public void shouldRunTheBlockCommand() throws IOException, ExecutionException, InterruptedException {
        final class Keys implements AsyncBlocker.Result {
            public int nums = -1;

            @Override
            public void close() {
                nums = -1;
            }
        }

        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            var selector = Selector.open();

            var noSelectedKeys = vExecutor.submit(() -> {
                var blocker = AsyncBlocker.create(new Keys(), (delay, unit, result) -> {
                    vExecutor.execute(selector::wakeup);
                    if (delay != 1 && unit != TimeUnit.HOURS) {
                        throw new IllegalStateException("parameters not correctly transferred");
                    }
                    selector.select(unit.toMillis(delay));
                    result.nums = 42;
                });
                try (var result = blocker.block(1, TimeUnit.HOURS)) {
                    return result.result().nums;
                }
            });
            Assertions.assertEquals(42, noSelectedKeys.get());
        }
    }

    @Test
    public void shouldThrowExceptionWhileReadingResult() throws ExecutionException, InterruptedException {
        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            var exResult = vExecutor.submit(() -> {
                var blocker = AsyncBlocker.create(() -> {
                }, (delay, unit, result) -> {
                    throw new RuntimeException();
                });
                try (var result = blocker.block()) {
                    try {
                        result.result();
                        return null;
                    } catch (Throwable expected) {
                        return expected;
                    }
                }
            });
            Assertions.assertNotNull(exResult.get());
        }
    }

    @Test
    public void shouldTransferExceptionsOnBlockCommand() throws ExecutionException, InterruptedException {
        final class CustomEx extends RuntimeException {
            private final int errorNo;

            public CustomEx(int errorNo) {
                this.errorNo = errorNo;
            }

            @Override
            public synchronized Throwable fillInStackTrace() {
                // stackless please!
                return this;
            }
        }
        var thrownEx = new CustomEx(42);
        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            var exResult = vExecutor.submit(() -> {
                var blocker = AsyncBlocker.create(() -> {
                }, (delay, unit, result) -> {
                    throw thrownEx;
                });
                try (var result = blocker.block(1, TimeUnit.HOURS)) {
                    return result.exception();
                }
            });
            Assertions.assertSame(thrownEx, exResult.get());
        }
    }

}
