package io.github.loomania;

public interface VirtualEventLoopContext {

    // This is necessary?
    // If within the same carrier of the event loop a continuation is submitted,
    // no wakeups will be called and the block will NEVER be unblocked: we need to be sure we have nothing to do, for real
    // before blocking.
    boolean tryBlock();

    void yield();
}
