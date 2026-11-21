package org.flexlb.balance.schedulev2;

import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.main.MainQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.RequestWorkLoadEstimateResult;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RequestPreparePoolTest {

    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final MainQueue mainQueue = mock(MainQueue.class);
    private final PrefillPicker picker = mock(PrefillPicker.class);
    private final RequestWorkLoadEstimateResult inputs =
            RequestWorkLoadEstimateResult.success(CacheMatchResult.empty(CacheMatchSource.LOCAL_SYNC), List.of());

    @Test
    void queuedTaskSavesTheEstimateBeforePuttingTheRequestBack() {
        ExecutorService executor = mock(ExecutorService.class);
        Request request = request("1");
        when(picker.prepare(request.ctx(), RoleType.PREFILL, "g")).thenReturn(inputs);
        doAnswer(call -> {
            assertSame(inputs, request.getWorkLoadEstimate());
            return null;
        }).when(mainQueue).addBackAndWake(request);
        try (RequestPreparePool workers = new RequestPreparePool(executor, mainQueue, picker, RoleType.PREFILL)) {
            workers.submit(request, "g");
            assertNull(request.getWorkLoadEstimate());
            verifyNoInteractions(picker, mainQueue);

            ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
            verify(executor).execute(task.capture());
            task.getValue().run();
            verify(mainQueue).addBackAndWake(request);
            verify(picker).prepare(request.ctx(), RoleType.PREFILL, "g");
        }
    }

    @Test
    void cancelledRequestDoesNotStartQueuedPreparation() {
        ExecutorService executor = mock(ExecutorService.class);
        Request request = request("1");
        try (RequestPreparePool workers = new RequestPreparePool(executor, mainQueue, picker, RoleType.PREFILL)) {
            workers.submit(request, "g");
            request.cancel(CancelReason.CLIENT_CANCELLED);
            ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
            verify(executor).execute(task.capture());
            task.getValue().run();
            verifyNoInteractions(picker, mainQueue);
            assertEquals(Stage.CANCELLED, request.stage());
        }
    }

    @Test
    void errorDuringPreparationIsReturnedWithoutCountingARejectedSubmission() {
        AssertionError failure = new AssertionError("preparation failed");
        Request first = request("1");
        Request second = request("2");
        when(picker.prepare(first.ctx(), RoleType.PREFILL, "g")).thenThrow(failure);
        when(picker.prepare(second.ctx(), RoleType.PREFILL, "g")).thenReturn(inputs);
        try (RequestPreparePool workers =
                new RequestPreparePool(new DirectExecutorService(), mainQueue, picker, RoleType.PREFILL)) {
            workers.submit(first, "g");
            assertSame(failure, first.getWorkLoadEstimate().failure());
            assertEquals(0, first.ctx().getMetrics().getPrepareSubmitRejectedCount());
            verify(mainQueue).addBackAndWake(first);

            workers.submit(second, "g");
            assertSame(inputs, second.getWorkLoadEstimate());
            verify(mainQueue).addBackAndWake(second);
        }
    }

    private Request request(String id) {
        return V2TestSupport.request(config, id, 50, 16L);
    }
}
