package org.flexlb.balance.schedulev2;

import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.PrefillInputs;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.config.FlexlbConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RequestPreparePoolTest {

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final LoopSignal signal = new LoopSignal();
    private final PrefillInputs inputs =
            new PrefillInputs(CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC), List.of());

    @Test
    void sameRequestIsNotSubmittedAgainWhileItsPreparationIsPendingOrDone() {
        ExecutorService executor = mock(ExecutorService.class);
        Request request = request("1");
        try (RequestPreparePool workers = new RequestPreparePool(executor, signal)) {
            workers.submit(request, () -> inputs);
            var estimate = request.getWorkLoadEstimate();
            workers.submit(request, () -> inputs);
            assertSame(estimate, request.getWorkLoadEstimate());
            assertFalse(estimate.isDone());

            ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
            verify(executor).execute(task.capture());
            task.getValue().run();
            workers.submit(request, () -> inputs);
            verify(executor).execute(any());
            assertSame(estimate, request.getWorkLoadEstimate());
            assertSame(inputs, estimate.join().value());
        }
    }

    @Test
    void rejectedSubmissionLeavesRequestWaitingAndCanBeRetried() throws Exception {
        ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Request first = request("1");
        Request second = request("2");
        Request third = request("3");
        try (RequestPreparePool workers = new RequestPreparePool(executor, signal)) {
            workers.submit(first, () -> {
                started.countDown();
                await(finish);
                return inputs;
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            workers.submit(second, () -> inputs);
            workers.submit(third, () -> inputs);
            workers.submit(third, () -> inputs);
            assertEquals(2, third.ctx().getMetrics().getPrepareSubmitRejectedCount());
            assertEquals(Stage.MAIN_WAITING, third.stage());
            assertFalse(third.future().isDone());
            assertNull(third.getWorkLoadEstimate());

            finish.countDown();
            assertSame(inputs, second.getWorkLoadEstimate().get(5, TimeUnit.SECONDS).value());
            // The completed estimates do not consume executor capacity even if the main loop has not read them.
            workers.submit(third, () -> inputs);
            assertSame(inputs, third.getWorkLoadEstimate().get(5, TimeUnit.SECONDS).value());
            assertEquals(2, third.ctx().getMetrics().getPrepareSubmitRejectedCount());
        } finally {
            finish.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancelledRequestDoesNotStartQueuedPreparation() {
        ExecutorService executor = mock(ExecutorService.class);
        Request request = request("1");
        AtomicInteger preparations = new AtomicInteger();
        try (RequestPreparePool workers = new RequestPreparePool(executor, signal)) {
            workers.submit(request, () -> {
                preparations.incrementAndGet();
                return inputs;
            });
            request.cancel(CancelReason.CLIENT_CANCELLED);
            ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
            verify(executor).execute(task.capture());
            task.getValue().run();
            assertEquals(0, preparations.get());
            assertEquals(Stage.CANCELLED, request.stage());
        }
    }

    @Test
    void errorDuringPreparationIsReturnedWithoutCountingARejectedSubmission() {
        AssertionError failure = new AssertionError("preparation failed");
        try (RequestPreparePool workers = new RequestPreparePool(new DirectExecutorService(), signal)) {
            Request first = request("1");
            workers.submit(first, () -> {
                throw failure;
            });
            assertSame(failure, first.getWorkLoadEstimate().join().failure());
            assertEquals(0, first.ctx().getMetrics().getPrepareSubmitRejectedCount());

            Request second = request("2");
            workers.submit(second, () -> inputs);
            assertSame(inputs, second.getWorkLoadEstimate().join().value());
        }
    }

    private Request request(String id) {
        return V2TestSupport.request(config, id, 50, 16L);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError(error);
        }
    }
}
