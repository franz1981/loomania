package io.github.loomania;

import java.util.concurrent.Executor;

public interface EventLoopExecutor extends Executor, AutoCloseable {
    @Override
    void close();
}
