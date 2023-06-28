package io.github.loomania;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.*;

@Fork(2)
@Warmup(iterations = 10, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AsyncBlockerRoundTripBenchmark {

    private AsyncBlocker<SelectorResult> safeSelector;

    private static class SelectorResult implements AsyncBlocker.Result {
        int keys;

        @Override
        public void close() {
            keys = -1;
        }
    }

    private Selector selector;
    @Param({"0", "100"})
    private int ioWork;

    private ExecutorService asyncSelector;

    private Callable<Integer> asyncSelectTask;

    @Setup
    public void init() throws IOException {
        this.selector = Selector.open();
        var ioWork = this.ioWork;
        var localSelector = this.selector;
        safeSelector = AsyncBlocker.create(new SelectorResult(), new AsyncBlocker.BlockWithTimeout<>() {

            @Override
            public void block(long delay, TimeUnit unit, SelectorResult selectorResult) throws Throwable {
                localSelector.select(unit.toMillis(delay));
                selectorResult.keys = ioWork;
            }

            @Override
            public void block(final SelectorResult selectorResult) throws Throwable {
                localSelector.select();
                selectorResult.keys = ioWork;

            }
        });
        asyncSelector = Executors.newCachedThreadPool();
        asyncSelectTask = () -> {
          selector.select();
          return ioWork;
        };
    }

    @Benchmark
    public int vanillaSafeSelectAlreadyAwaken() throws ExecutionException, InterruptedException {
        selector.wakeup();
        Future<Integer> keysPromise = asyncSelector.submit(asyncSelectTask);
        int keys = keysPromise.get();
        if (keys > 0) {
            Blackhole.consumeCPU(keys);
        }
        return keys;
    }

    @Benchmark
    public int selectAlreadyAwaken() throws IOException {
        selector.wakeup();
        int keys = selector.select();
        if (keys > 0) {
            Blackhole.consumeCPU(keys);
        }
        return keys;
    }

    @Benchmark
    public int loomaniaSafeSelectAlreadyAwaken() {
        selector.wakeup();
        try (var tryResult = safeSelector.block()) {
            int keys = tryResult.result().keys;
            if (keys > 0) {
                Blackhole.consumeCPU(keys);
            }
            return keys;
        }
    }

    @Benchmark
    public int loomaniaSafeSelectAlreadyAwakenSpinWait() {
        selector.wakeup();
        try (var tryResult = safeSelector.block(true)) {
            int keys = tryResult.result().keys;
            if (keys > 0) {
                Blackhole.consumeCPU(keys);
            }
            return keys;
        }
    }

    @TearDown
    public void stopSelector() throws IOException {
        selector.close();
        asyncSelector.shutdown();
    }

}
