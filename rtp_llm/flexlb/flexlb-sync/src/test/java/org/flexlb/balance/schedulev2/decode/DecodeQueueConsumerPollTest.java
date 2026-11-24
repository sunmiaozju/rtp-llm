package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class DecodeQueueConsumerPollTest {

    @Test
    void startsConfiguredThreadsAndWaitsForTheirExit() throws Exception {
        List<DecodeQueueConsumer> consumers = new ArrayList<>();
        ConcurrentLinkedQueue<Thread> threads = new ConcurrentLinkedQueue<>();
        CountDownLatch started = new CountDownLatch(4);
        CountDownLatch finish = new CountDownLatch(1);
        DecodeQueueConsumerPoll pool =
                new DecodeQueueConsumerPoll(4, new DecodeQueue(false), new PlacementAvailability(), () -> {
                    DecodeQueueConsumer consumer = mock(DecodeQueueConsumer.class);
                    doAnswer(call -> {
                        threads.add(Thread.currentThread());
                        started.countDown();
                        assertTrue(finish.await(5, TimeUnit.SECONDS));
                        return null;
                    }).when(consumer).run();
                    doAnswer(call -> {
                        finish.countDown();
                        return null;
                    }).when(consumer).close();
                    consumers.add(consumer);
                    return consumer;
                });
        try {
            pool.start();
            pool.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
        } finally {
            finish.countDown();
            pool.close();
        }
        assertEquals(4, threads.size());
        assertEquals(4L, threads.stream().map(Thread::getName).distinct().count());
        for (Thread thread : threads) {
            assertTrue(thread.getName().startsWith("flexlb-v2-decode-consumer-"));
            assertFalse(thread.isAlive());
        }
        for (DecodeQueueConsumer consumer : consumers) {
            verify(consumer).run();
            verify(consumer).close();
        }
    }

    @Test
    void keepsClusterListenerWithoutWakingConsumersAndRemovesItOnClose() {
        DecodeQueue queue = new DecodeQueue(false);
        PlacementAvailability availability = spy(new PlacementAvailability());
        DecodeQueueConsumerPoll pool =
                new DecodeQueueConsumerPoll(1, queue, availability, () -> mock(DecodeQueueConsumer.class));
        pool.start();
        try {
            long version = queue.changeVersion();
            availability.capacityChanged(RoleType.DECODE, "g", "d2");
            availability.topologyChanged(RoleType.DECODE, "g", "d3");
            availability.capacityChanged(RoleType.PREFILL, "g", "p1");
            assertEquals(version, queue.changeVersion());
        } finally {
            pool.close();
        }
        var listener = ArgumentCaptor.forClass(PlacementAvailability.Listener.class);
        verify(availability).addListener(listener.capture());
        verify(availability).removeListener(listener.getValue());
    }

    @Test
    void closeBeforeStartDoesNotLaunchConsumer() {
        DecodeQueueConsumer consumer = mock(DecodeQueueConsumer.class);
        DecodeQueueConsumerPoll pool =
                new DecodeQueueConsumerPoll(1, new DecodeQueue(false), new PlacementAvailability(), () -> consumer);
        pool.close();
        pool.start();
        pool.close();
        verify(consumer, never()).run();
        verify(consumer).close();
    }
}
