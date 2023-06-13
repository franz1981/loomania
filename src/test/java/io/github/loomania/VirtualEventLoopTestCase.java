package io.github.loomania;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class VirtualEventLoopTestCase {

    private static Stream<Arguments> setupEventLoop() {
        final Arguments virtual;
        {
            var eventLoop = new VirtualLocalEventLoop();
            var vtExecutorService = LoomaniaController.newEventLoopExecutorService(Thread::new,
                    eventLoop::wakeup, eventLoop::eventLoop, ExecutorServiceListener.EMPTY);
            virtual = Arguments.of(vtExecutorService, eventLoop);
        }
        final Arguments managed;
        {
            var builder = Loomania.newJdkVirtualThreadExecutorBuilder().setCorePoolSize(1);
            var vtExecutorService = builder.build();
            var eventLoop = new ManagedSelectorEventLoop(vtExecutorService);
            managed = Arguments.of(vtExecutorService, eventLoop);
        }
        return Stream.of(virtual, managed);
    }

    @Test
    public void eventLoopFJ() throws InterruptedException {
        try (var vExecutor = Executors.newVirtualThreadPerTaskExecutor();
             var localEventLoop = new LocalEventLoop(vExecutor)) {
            final var done = new CountDownLatch(1);
            localEventLoop.execute(() -> {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                done.countDown();
            });
            done.await();
        }
    }

    @ParameterizedTest(name = "{index} => {1}")
    @MethodSource("setupEventLoop")
    public void shouldYieldBeFair(ExecutorService virtualExecutor, EventLoopExecutor eventLoop) throws InterruptedException, ExecutionException {
        var lock = new ReentrantLock(true);
        var acquisitionOrder = new AtomicInteger();
        var eventLoopLockOrder = new CompletableFuture<Integer>();
        var loomaniaLockOrder = new CompletableFuture<Integer>();
        lock.lock();
        eventLoop.execute(() -> {
            virtualExecutor.execute(() -> {
                lock.lock();
                var order = acquisitionOrder.incrementAndGet();
                lock.unlock();
                loomaniaLockOrder.complete(order);
            });
            lock.lock();
            var order = acquisitionOrder.incrementAndGet();
            lock.unlock();
            eventLoopLockOrder.complete(order);
        });
        while (lock.getQueueLength() != 2) {
            Thread.yield();
        }
        lock.unlock();
        Assertions.assertEquals(1, eventLoopLockOrder.get());
        Assertions.assertEquals(2, loomaniaLockOrder.get());
        eventLoop.close();
        virtualExecutor.close();
    }

    @ParameterizedTest(name = "{index} => {1}")
    @MethodSource("setupEventLoop")
    public void shouldNotDeadlock(ExecutorService virtualExecutor, EventLoopExecutor eventLoop) throws InterruptedException {
        var lock = new ReentrantLock(true);
        var lockAcquired = new CompletableFuture<Boolean>();
        var bothAcquireAndRelease = new CountDownLatch(2);

        lock.lock();
        eventLoop.execute(() -> {
            virtualExecutor.execute(() -> {
                eventLoop.execute(() -> {
                    assert lock.isLocked();
                    lock.lock();
                    lock.unlock();
                    bothAcquireAndRelease.countDown();
                });
                lockAcquired.join();
                assert lock.isLocked();
                lock.lock();
                lock.unlock();
                bothAcquireAndRelease.countDown();
            });
        });
        try {
            lockAcquired.complete(true);
            while (lock.getQueueLength() != 2) {
                Thread.yield();
            }
        } finally {
            lock.unlock();
            bothAcquireAndRelease.await();
        }
        eventLoop.close();
        virtualExecutor.close();
    }

    @ParameterizedTest(name = "{index} => {1}")
    @MethodSource("setupEventLoop")
    public void eventLoopLoomania(ExecutorService virtualExecutor, EventLoopExecutor eventLoop) throws InterruptedException {
        final var done = new CountDownLatch(1);
        eventLoop.execute(() -> {
            System.out.println("ISSUING A V THREAD BLOCKING CALL");
            // emulate a rest endpoint that can issue a blocking call
            virtualExecutor.execute(() -> {
                try {
                    done.await();
                    System.out.println("DONE ON V THREAD");
                } catch (Throwable ignore) {
                    ignore.printStackTrace();
                }
            });
        });
        System.out.println("AWAIT 10 SECS");
        TimeUnit.SECONDS.sleep(10);
        System.out.println("ASYNC DONE");
        // LOOMANIA IS SUSPENDED HERE WHILE IT SHOULDN'T!!!
        eventLoop.execute(() -> {
            System.out.println("DONE STARTED");
            done.countDown();
            System.out.println("DONE COMPLETED");
        });
        done.await();
        eventLoop.close();
        virtualExecutor.close();
    }
}
