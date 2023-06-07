package io.github.loomania;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class LocalEventLoopTestCase {

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

    @Test
    public void shouldYieldBeFair() throws InterruptedException, ExecutionException {
        var lock = new ReentrantLock(true);
        var eventLoop = new VirtualLocalEventLoop();
        var loomania = LoomaniaController.newEventLoopExecutorService(Thread::new,
                eventLoop::wakeup, eventLoop::eventLoop, ExecutorServiceListener.EMPTY);

        var acquisitionOrder = new AtomicInteger();
        var eventLoopLockOrder = new CompletableFuture<Integer>();
        var loomaniaLockOrder = new CompletableFuture<Integer>();
        lock.lock();
        eventLoop.execute(() -> {
            loomania.execute(() -> {
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
        loomania.close();
    }

    @Test
    public void shouldNotDeadlock() throws InterruptedException {
        var lock = new ReentrantLock(true);
        var lockAcquired = new CompletableFuture<Boolean>();
        var bothAcquireAndRelease = new CountDownLatch(2);

        var eventLoop = new VirtualLocalEventLoop();
        var loomania = LoomaniaController.newEventLoopExecutorService(Thread::new,
                eventLoop::wakeup, eventLoop::eventLoop, ExecutorServiceListener.EMPTY);

        lock.lock();
        eventLoop.execute(() -> {
            loomania.execute(() -> {
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
        loomania.close();
    }

    @Test
    public void eventLoopLoomania() throws InterruptedException {
        var eventLoop = new VirtualLocalEventLoop();
        var loomania = LoomaniaController.newEventLoopExecutorService(Thread::new,
                     eventLoop::wakeup, eventLoop::eventLoop, ExecutorServiceListener.EMPTY);
        final var done = new CountDownLatch(1);
        eventLoop.execute(() -> {
            System.out.println("ISSUING A V THREAD BLOCKING CALL");
            // emulate a rest endpoint that can issue a blocking call
            loomania.execute(() -> {
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
        loomania.close();
    }
}
