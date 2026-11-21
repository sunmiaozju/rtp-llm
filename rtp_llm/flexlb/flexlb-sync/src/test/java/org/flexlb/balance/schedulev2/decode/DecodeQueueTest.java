package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DecodeQueueTest {

    @Test
    void addingRequestWakesWaitingConsumer() throws Exception {
        DecodeQueue queue = new DecodeQueue(false);
        Request head = mock(Request.class);
        when(head.stage()).thenReturn(Stage.DECODE_WAITING);
        CountDownLatch started = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var taken = executor.submit(() -> queue.take(() -> {
                started.countDown();
                return true;
            }));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            queue.addWaiting(head);
            assertSame(head, taken.get(5, TimeUnit.SECONDS));
            assertEquals(0, queue.waitingCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void closingWakesAllWaitingConsumers() throws Exception {
        DecodeQueue queue = new DecodeQueue(false);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch started = new CountDownLatch(2);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> {
                started.countDown();
                return queue.take(running::get);
            });
            var second = executor.submit(() -> {
                started.countDown();
                return queue.take(running::get);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            running.set(false);
            queue.wake();
            assertNull(first.get(5, TimeUnit.SECONDS));
            assertNull(second.get(5, TimeUnit.SECONDS));
        } finally {
            running.set(false);
            queue.wake();
            executor.shutdownNow();
        }
    }

    @Test
    void requestCancelledDuringInsertionIsDiscardedOnNextTake() throws InterruptedException {
        DecodeQueue queue = new DecodeQueue(false);
        Request cancelled = mock(Request.class);
        when(cancelled.queueSeq()).thenReturn(1L);
        // 模拟入队检查后立即取消，允许已结束记录暂留队列，下一次出队再丢弃。
        when(cancelled.stage()).thenReturn(Stage.DECODE_WAITING, Stage.CANCELLED);
        queue.addWaiting(cancelled);
        assertEquals(1, queue.waitingCount());

        Request ready = mock(Request.class);
        when(ready.queueSeq()).thenReturn(2L);
        when(ready.stage()).thenReturn(Stage.DECODE_WAITING);
        queue.addWaiting(ready);
        Request taken = queue.take(() -> true);

        assertSame(ready, taken);
        assertEquals(0, queue.waitingCount());
    }
}
