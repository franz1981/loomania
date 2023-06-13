package io.github.loomania;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

final class SynchronousRandevouz<T> {
    public static class Peer<T> {
        public static final long NULL_HEAD_ID = 0;
        private static final VarHandle NEXT;
        private static final VarHandle HEAD_ID;
        private static final VarHandle ASLEEP;

        static {
            try {
                NEXT = MethodHandles.lookup().in(Peer.class).findVarHandle(Peer.class, "next", Peer.class);
                ASLEEP = MethodHandles.lookup().in(Peer.class).findVarHandle(Peer.class, "asleep", Thread.class);
                HEAD_ID = MethodHandles.lookup().in(Peer.class).findVarHandle(Peer.class, "headId", long.class);
            } catch (Throwable e) {
                throw new Error(e);
            }
        }

        private T value;
        private Thread asleep;
        private Peer next;
        private long headId = NULL_HEAD_ID;

        public Peer() {

        }

        public boolean isAcquireHead() {
            return ((long) HEAD_ID.getAcquire(this)) > NULL_HEAD_ID;
        }

        public long getAcquireHeadId() {
            return (long) HEAD_ID.getAcquire(this);
        }

        public void setReleaseHeadId(long value) {
            HEAD_ID.setRelease(this, value);
        }

        public boolean tryNullOutHeadId(long expected) {
            if (expected <= NULL_HEAD_ID) {
                throw new IllegalArgumentException("null out headId shouldn't ");
            }
            return HEAD_ID.compareAndSet(this, expected, NULL_HEAD_ID);
        }

        public Thread getOpaqueAndNullReleaseAsleep() {
            Thread sleeping = (Thread) ASLEEP.getOpaque(this);
            ASLEEP.setRelease(this, null);
            return sleeping;
        }

        public void setOpaqueAsleep(Thread toSleep) {
            ASLEEP.setOpaque(this, toSleep);
        }

        public Thread getAcquireAsleep() {
            return (Thread) ASLEEP.getAcquire(this);
        }

        public Peer<T> getAcquireNext() {
            return (Peer<T>) NEXT.getAcquire(this);
        }

        public void setReleaseNext(Peer next) {
            NEXT.setRelease(this, next);
        }

        public void wakeupForSync() {
            // no need to load acquire asleep: a previous loadAcquireNext ensure to have asleep valued
            var awakeIt = getOpaqueAndNullReleaseAsleep();
            assert awakeIt != null;
            LockSupport.unpark(awakeIt);
        }

        public void awaitSync() {
            for (; ; ) {
                LockSupport.park();
                if (getAcquireAsleep() == null) {
                    return;
                }
            }
        }
    }

    // TODO: we can save false-sharing by adding some padding
    private static final VarHandle P_NODE;
    private static final VarHandle C_NODE;

    static {
        try {
            P_NODE = MethodHandles.lookup().in(Peer.class).findVarHandle(SynchronousRandevouz.class, "pNode", Peer.class);
            C_NODE = MethodHandles.lookup().in(Peer.class).findVarHandle(SynchronousRandevouz.class, "cNode", Peer.class);
        } catch (Throwable e) {
            throw new Error(e);
        }
    }

    private Peer<T> pNode;
    private Peer<T> cNode;

    public SynchronousRandevouz() {
        var node = new Peer();
        node.setReleaseHeadId(1);
        pNode = node;
        cNode = node;
        VarHandle.storeStoreFence();
    }

    public boolean transferToPeer(T item) {
        Objects.requireNonNull(item);
        for (; ; ) {
            var first = (Peer<T>) C_NODE.getAcquire(this);
            var nodeId = first.getAcquireHeadId();
            if (nodeId == Peer.NULL_HEAD_ID) {
                // current head isn't head anymore
                continue;
            }
            // nodeId is both used to detect if first::next is stable
            // and to get ownership of
            var next = first.getAcquireNext();
            // we need to make sure if first is fully constructed and not GC'ed: see offerPeerToSync
            if (next != null && first != next) {
                assert nodeId >= Peer.NULL_HEAD_ID;
                // if succeed other concurrent producers would spin till things change
                if (!first.tryNullOutHeadId(nodeId)) {
                    // next is not something we can rely on anymore: the world is moving!
                    continue;
                }
                assert next.getAcquireHeadId() == Peer.NULL_HEAD_ID;
                first.setReleaseNext(first);
                // other concurrent producers would spin on next, because not yet head
                C_NODE.setRelease(this, next);
                // unblock concurrent producers on next: they can now navigate to it
                // and eventually perform another transfer (ie involving head change, again)
                next.setReleaseHeadId(nodeId + 1);
                // wakeupForSync is null store-release asleep and publishing value too
                next.value = item;
                next.wakeupForSync();
                return true;
            } else if (first == P_NODE.getAcquire(this) && first.isAcquireHead()) {
                // no available peers to sync with
                return false;
            }
        }
    }

    public T offerPeerAndTake(Peer<T> peer) {
        assert peer.getAcquireNext() == null || peer.getAcquireNext() == peer;
        var prevLast = (Peer<T>) P_NODE.getAndSet(this, peer);
        // we need to fully setup peer *before* a transmitter can reach it
        // prevLast::setReleaseNext would store release asleep as well
        peer.setOpaqueAsleep(Thread.currentThread());
        // linking to peer would "unblock" a transmitter to find it
        prevLast.setReleaseNext(peer);
        // we await till we are really awaken
        peer.awaitSync();
        // we can use a plain load on value because asleep load-acquire it
        var transferredValue = peer.value;
        // value transferred, clean this mess up!
        peer.value = null;
        return transferredValue;
    }
}
