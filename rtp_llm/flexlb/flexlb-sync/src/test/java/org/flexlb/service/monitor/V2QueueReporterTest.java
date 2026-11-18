package org.flexlb.service.monitor;

import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.V2TestSupport;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestTable;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceMetrics.PrefillRouteMetrics;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.flexlb.constant.MetricConstant.CACHE_AFFINITY_DECISION;
import static org.flexlb.constant.MetricConstant.CACHE_AFFINITY_OVER_CAP_MS;
import static org.flexlb.constant.MetricConstant.CACHE_FIND_MATCHING_ENGINES_RT;
import static org.flexlb.constant.MetricConstant.CACHE_ROUTING_CANDIDATE_MAX_HIT_TOKENS;
import static org.flexlb.constant.MetricConstant.CACHE_ROUTING_SELECTED_MATCH_HIT_TOKENS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V2QueueReporterTest {

    private final FlexMonitor monitor = mock(FlexMonitor.class);
    private final AsyncRequestMetrics asyncRequestMetrics = new AsyncRequestMetrics(monitor);
    private final V2QueueReporter reporter = new V2QueueReporter(monitor, asyncRequestMetrics);
    private final FlexlbConfig config = SchedulingTestConfig.newConfig();
    private final RequestTable table = new RequestTable();
    private final ConfigService service = mock(ConfigService.class);
    private final PrefillQueueRegistry prefills = new PrefillQueueRegistry();

    V2QueueReporterTest() {
        config.getDispatcher().setType(org.flexlb.config.DispatcherConfig.Type.NON_BATCH_V2);
        when(service.loadBalanceConfig()).thenReturn(config);
    }

    @AfterEach
    void stopReporter() {
        asyncRequestMetrics.closeReportExecutor();
    }

    @Test
    void preemptedReturnReportsQueueMovementAndBothPriorities() {
        reporter.preemptedReturn(50, 40, "p1:8000");
        verify(monitor).report(org.flexlb.constant.MetricConstant.AUTO_TPM_VICTIM_COUNT,
                FlexMetricTags.of("case", "prefill_return_to_main", "stage", "prefill", "action", "requeue",
                        "from_queue", "prefill", "to_queue", "main", "incoming_priority", "50", "victim_priority",
                        "40"),
                1.0);
        org.mockito.Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void mainEvictionAndDecodeInstructionsHaveDistinctScenariosAndActions() {
        reporter.mainPreempted(50, 40);
        verify(monitor).report(org.flexlb.constant.MetricConstant.AUTO_TPM_VICTIM_COUNT,
                FlexMetricTags.of("case", "main_evicted", "stage", "main", "from_queue", "main", "to_queue", "none",
                        "action", "reject", "incoming_priority", "50", "victim_priority", "40"),
                1.0);
        for (var phase : List.of(org.flexlb.enums.DecodeTaskPhase.RUNNING,
                org.flexlb.enums.DecodeTaskPhase.ACCEPTED_NOT_RUNNING,
                org.flexlb.enums.DecodeTaskPhase.ENGINE_MAY_HAVE_SEEN)) {
            String scenario = switch (phase) {
                case RUNNING -> "decode_running";
                case ACCEPTED_NOT_RUNNING -> "decode_accepted";
                default -> "decode_dispatched_unconfirmed";
            };
            reporter.decodePreempted(50, 40, phase, "d1:8000");
            verify(monitor).report(org.flexlb.constant.MetricConstant.AUTO_TPM_VICTIM_COUNT,
                    FlexMetricTags.of("case", scenario, "stage", "decode_engine", "from_queue", "decode_engine",
                            "to_queue", "none", "action", "return_cancel_instruction", "incoming_priority", "50",
                            "victim_priority", "40"),
                    1.0);
        }
        org.mockito.Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void countsActualStagesAndClearsRemovedEngineSeries() {
        Request main = add("1");
        Request prefill = add("2");
        V2TestSupport.choose(prefill, V2TestSupport.prefillEndpoint("p1:8000"), 0L, 10L);
        prefill.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        V2TestSupport.register(prefills, prefill.selectedPrefill().endpoint());
        prefills.addWaiting(prefill);
        Request decode = add("3");
        decode.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        decode.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        reporter.sample(table, List.of("p1:8000", "p2:8000"));
        depth("main", "all", 1);
        depth("prefill", "p1:8000", 1);
        depth("prefill", "p2:8000", 0);
        depth("decode_waiting", "all", 1);
        depth("decode_reserved", "all", 0);

        clearInvocations(monitor);
        main.cancel(CancelReason.CLIENT_CANCELLED);
        prefill.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        prefills.remove(prefills.find(prefill));
        decode.moveTo(Stage.DECODE_WAITING, Stage.DECODE_RESERVED);
        reporter.sample(table, List.of());
        depth("main", "all", 0);
        depth("prefill", "p1:8000", 0);
        depth("decode_waiting", "all", 1);
        depth("decode_reserved", "all", 1);

        clearInvocations(monitor);
        prefill.publish(Stage.DECODE_WAITING, List.of());
        decode.publish(Stage.DECODE_RESERVED, List.of());
        reporter.sample(table, List.of());
        depth("decode_waiting", "all", 0);
        depth("decode_reserved", "all", 0);
        verify(monitor, never()).report(eq(V2QueueReporter.DEPTH),
                eq(FlexMetricTags.of("queue", "prefill", "endpoint", "p1:8000")), anyDouble());
        verify(monitor, never()).report(eq(V2QueueReporter.DEPTH),
                eq(FlexMetricTags.of("queue", "prefill", "endpoint", "p2:8000")), anyDouble());
    }

    @Test
    void retriesCountEveryAttemptButOnlyOneBlockedRequestUntilReasonOrStageChanges() {
        Request request = add("1");
        for (int i = 0; i < 10; i++) {
            reporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL, "p1:8000");
        }
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        request.moveTo(Stage.PREFILL_WAITING, Stage.MAIN_WAITING);
        reporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL, "p1:8000");

        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        reporter.blocked(request, Stage.PREFILL_WAITING, QueueBlockReason.PREFILL_INFLIGHT_FULL, "p1:8000");
        verify(monitor, never()).report(eq(V2QueueReporter.ATTEMPTS), any(), anyDouble());
        verify(monitor, never()).report(eq(V2QueueReporter.BLOCKED), any(), anyDouble());
        request.fail(StrategyErrorType.DISPATCH_FAILED, "test failure");
        reporter.completed(request, request.future().join());

        verify(monitor, timeout(5000)).report(V2QueueReporter.WAIT,
                FlexMetricTags.of("queue", "decode", "result", "failed"), 0.0);
        verify(monitor, times(2)).report(eq(V2QueueReporter.ATTEMPTS), any(), anyDouble());
        verify(monitor, times(2)).report(eq(V2QueueReporter.BLOCKED), any(), anyDouble());
        var main = FlexMetricTags.of("queue", "main", "reason", "SHARED_QUEUE_FULL", "priority", "50");
        var prefill = FlexMetricTags.of("queue", "prefill", "reason", "PREFILL_INFLIGHT_FULL", "priority", "50");
        verify(monitor).report(V2QueueReporter.ATTEMPTS, main, 11.0);
        verify(monitor).report(V2QueueReporter.ATTEMPTS, prefill, 1.0);
        verify(monitor).report(V2QueueReporter.BLOCKED, main, 2.0);
        verify(monitor).report(V2QueueReporter.BLOCKED, prefill, 1.0);
    }

    @Test
    void rejectionIsAttributedToQueueBeforeFailureAndPvTimesExistBeforeResponse() throws Exception {
        Request request = add("1");
        var observed = request.future().thenAccept(response -> {
            assertEquals(request.queueProgress().mainMs(), request.ctx().getGlobalQueueMs());
            assertEquals(request.queueProgress().prefillMs(), request.ctx().getPrefillQueueMs());
            reporter.completed(request, response);
        });
        Thread.sleep(10L);
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        Thread.sleep(10L);
        request.fail(StrategyErrorType.QUEUE_FULL_LOW_PRIORITY_REJECTED, "queue full");
        observed.join();
        verify(monitor, timeout(5000)).report(V2QueueReporter.REJECTED,
                FlexMetricTags.of("queue", "prefill", "reason", "QUEUE_FULL_LOW_PRIORITY_REJECTED", "priority", "50"),
                1.0);
        assertTrue(request.ctx().getGlobalQueueMs() > 0);
        assertTrue(request.ctx().getPrefillQueueMs() > 0);
    }

    @Test
    void endpointChangesKeepBlockDeduplicationButShareMetricTags() {
        Request request = add("1");
        reporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL, "p1:8000");
        reporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL, "p2:8000");
        FlexMetricTags tags = FlexMetricTags.of("queue", "main", "reason", "SHARED_QUEUE_FULL", "priority", "50");
        request.fail(StrategyErrorType.DISPATCH_FAILED, "test failure");
        reporter.completed(request, request.future().join());
        verify(monitor, timeout(5000)).report(V2QueueReporter.BLOCKED, tags, 2.0);
        verify(monitor).report(V2QueueReporter.ATTEMPTS, tags, 2.0);
    }

    @Test
    void blockedMetricsWorkerDoesNotBlockCompletionAndDropsAreVisible() throws Exception {
        var executor = (ThreadPoolExecutor) ReflectionTestUtils.getField(asyncRequestMetrics, "reportExecutor");
        int processors = Runtime.getRuntime().availableProcessors();
        assertEquals(processors, executor.getCorePoolSize());
        assertEquals(processors * 3, executor.getMaximumPoolSize());
        executor.setCorePoolSize(1);
        executor.setMaximumPoolSize(3);

        var reportThread = new CompletableFuture<Thread>();
        var releaseReport = new CountDownLatch(1);
        doAnswer(invocation -> {
            reportThread.complete(Thread.currentThread());
            releaseReport.await(10, TimeUnit.SECONDS);
            return null;
        }).when(monitor).report(eq(V2QueueReporter.WAIT), any(), anyDouble());

        var tags = FlexMetricTags.of("reason", "queue_full");
        try {
            reportSuccessfulCompletion("active");
            assertNotSame(Thread.currentThread(), reportThread.get(5, TimeUnit.SECONDS));
            int queueCapacity = executor.getQueue().remainingCapacity();
            Runnable queuedTask = () -> {};
            for (int i = 0; i < queueCapacity; i++) {
                executor.getQueue().add(queuedTask);
            }
            reportSuccessfulCompletion("extra-worker-1");
            reportSuccessfulCompletion("extra-worker-2");
            assertEquals(3, executor.getPoolSize());
            reportSuccessfulCompletion("queue-full");
            asyncRequestMetrics.reportCacheMatch(1_500L);
            verify(monitor, never()).report(eq(CACHE_FIND_MATCHING_ENGINES_RT), any(), anyDouble());
            verify(monitor, never()).report(eq(AsyncRequestMetrics.DROPPED_REPORTS), any(), anyDouble());
            asyncRequestMetrics.reportDroppedQueueFull();
            verify(monitor).report(AsyncRequestMetrics.DROPPED_REPORTS, tags, 2.0);

            asyncRequestMetrics.closeReportExecutor();
            reportSuccessfulCompletion("shutdown");
            asyncRequestMetrics.reportDroppedQueueFull();
            verify(monitor).report(AsyncRequestMetrics.DROPPED_REPORTS, tags, 0.0);
        } finally {
            releaseReport.countDown();
        }
    }

    @Test
    void cacheMatchReportsRawMicrosecondsWithoutTagsOnMetricsWorker() throws Exception {
        var reportThread = new CompletableFuture<Thread>();
        doAnswer(invocation -> {
            reportThread.complete(Thread.currentThread());
            return null;
        }).when(monitor).report(CACHE_FIND_MATCHING_ENGINES_RT, FlexMetricTags.of(), 1_750.0);

        asyncRequestMetrics.reportCacheMatch(1_750L);
        assertNotSame(Thread.currentThread(), reportThread.get(5L, TimeUnit.SECONDS));
        finishReports();
        verify(monitor).report(CACHE_FIND_MATCHING_ENGINES_RT, FlexMetricTags.of(), 1_750.0);
        org.mockito.Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void successfulCompletionReportsOnlyLatestPrefillSampleWithoutRoleTags() throws Exception {
        Request request = add("1");
        var metrics = request.ctx().getMetrics();
        metrics.setPrefillRouteMetrics(new PrefillRouteMetrics(900L, 1_000L, "CACHE_LEADER", null));
        metrics.setPrefillRouteMetrics(new PrefillRouteMetrics(128L, 256L, "OVER_CAP", 15L));
        request.future().thenAccept(response -> reporter.completed(request, response));
        assertTrue(request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING));
        assertTrue(request.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING));
        assertTrue(request.publish(Stage.DECODE_WAITING, List.of()));
        finishReports();

        verify(monitor).report(CACHE_ROUTING_SELECTED_MATCH_HIT_TOKENS, FlexMetricTags.of(), 128.0);
        verify(monitor).report(CACHE_ROUTING_CANDIDATE_MAX_HIT_TOKENS, FlexMetricTags.of(), 256.0);
        verify(monitor).report(CACHE_AFFINITY_DECISION, FlexMetricTags.of("decision", "OVER_CAP"), 1.0);
        verify(monitor).report(CACHE_AFFINITY_OVER_CAP_MS, FlexMetricTags.of(), 15.0);
        verify(monitor, times(3)).report(eq(V2QueueReporter.WAIT), any(), anyDouble());
        org.mockito.Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void failedCancelledAndTimedOutRequestsDoNotReportPrefillSamples() throws Exception {
        Request failed = add("1");
        Request cancelled = add("2");
        Request timedOut = add("3");
        for (Request request : List.of(failed, cancelled, timedOut)) {
            request.ctx().getMetrics().setPrefillRouteMetrics(new PrefillRouteMetrics(128L, 256L, "OVER_CAP", 15L));
            request.future().thenAccept(response -> reporter.completed(request, response));
        }
        assertTrue(failed.fail(StrategyErrorType.DISPATCH_FAILED, "test failure"));
        assertTrue(cancelled.cancel(CancelReason.CLIENT_CANCELLED));
        assertTrue(timedOut.cancel(CancelReason.DEADLINE_EXCEEDED));
        finishReports();

        verify(monitor, times(3)).report(eq(V2QueueReporter.REJECTED), any(), anyDouble());
        verify(monitor, times(9)).report(eq(V2QueueReporter.WAIT), any(), anyDouble());
        org.mockito.Mockito.verifyNoMoreInteractions(monitor);
    }

    @Test
    void monitoringFailureDoesNotFailScheduling() {
        doThrow(new IllegalStateException("monitor unavailable")).when(monitor).report(any(), any(), anyDouble());
        reporter.blocked(add("1"), Stage.MAIN_WAITING, QueueBlockReason.NO_AVAILABLE_PREFILL, "all");
        reporter.sample(table, List.of());
    }

    @Test
    void allQueueMetricsUseTwentySecondAggregation() {
        asyncRequestMetrics.init();
        reporter.init();
        int registered = 0;
        for (var invocation : org.mockito.Mockito.mockingDetails(monitor).getInvocations()) {
            if (invocation.getMethod().getName().equals("register")) {
                assertEquals(org.flexlb.enums.FlexPriorityType.NORMAL, invocation.getArgument(2));
                registered++;
            }
        }
        assertEquals(9, registered);
    }

    @Test
    void deadlineIsCountedOnceInItsQueueAndIsDifferentFromClientCancellation() {
        Request request = add("1");
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        request.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        request.moveTo(Stage.DECODE_WAITING, Stage.DECODE_RESERVED);
        var observed = request.future().thenAccept(response -> reporter.completed(request, response));
        assertTrue(request.cancel(CancelReason.DEADLINE_EXCEEDED));
        assertFalse(request.cancel(CancelReason.DEADLINE_EXCEEDED));
        observed.join();
        verify(monitor, timeout(5000)).report(V2QueueReporter.REJECTED,
                FlexMetricTags.of("queue", "decode_reserved", "reason", "REQUEST_TIMEOUT", "priority", "50"), 1.0);

        Request cancelled = add("2");
        cancelled.cancel(CancelReason.CLIENT_CANCELLED);
        reporter.completed(cancelled, null);
        verify(monitor, timeout(5000)).report(V2QueueReporter.REJECTED,
                FlexMetricTags.of("queue", "main", "reason", "REQUEST_CANCELLED", "priority", "50"), 1.0);
    }

    @Test
    void prefillInflightReportsLimitChangesAndClearsRemovedEndpoint() {
        var endpoint = V2TestSupport.prefillEndpoint("p1:8000");
        var queue = endpoint.prefillQueue();
        V2TestSupport.report(endpoint, 1L, 1_000L, 1_000L,
                java.util.Map.of("running", org.flexlb.enums.TaskPhase.RUNNING));
        FlexMetricTags tags = FlexMetricTags.of("endpoint", endpoint.ipPort());
        reporter.samplePrefillInflight(List.of(queue), 4);
        verify(monitor).report(V2QueueReporter.PREFILL_INFLIGHT, tags, 1.0);
        verify(monitor).report(V2QueueReporter.PREFILL_INFLIGHT_LIMIT, FlexMetricTags.of(), 4.0);
        clearInvocations(monitor);

        reporter.samplePrefillInflight(List.of(queue), 8);
        verify(monitor).report(V2QueueReporter.PREFILL_INFLIGHT_LIMIT, FlexMetricTags.of(), 8.0);
        clearInvocations(monitor);

        reporter.samplePrefillInflight(List.of(), 8);
        verify(monitor).report(V2QueueReporter.PREFILL_INFLIGHT, tags, 0.0);
        verify(monitor).report(V2QueueReporter.PREFILL_INFLIGHT_LIMIT, FlexMetricTags.of(), 8.0);

        clearInvocations(monitor);
        reporter.samplePrefillInflight(List.of(), 8);
        verify(monitor, never()).report(eq(V2QueueReporter.PREFILL_INFLIGHT), eq(tags), anyDouble());
    }

    private void finishReports() throws InterruptedException {
        asyncRequestMetrics.closeReportExecutor();
        var executor = (ThreadPoolExecutor) ReflectionTestUtils.getField(asyncRequestMetrics, "reportExecutor");
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private void reportSuccessfulCompletion(String id) {
        Request request = add(id);
        request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING);
        request.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING);
        assertTrue(request.publish(Stage.DECODE_WAITING, List.of()));
        reporter.completed(request, request.future().join());
    }

    private Request add(String id) {
        Request request = V2TestSupport.request(config, id, 50, 16L);
        table.add(request);
        return request;
    }

    private void depth(String queue, String endpoint, double value) {
        verify(monitor).report(V2QueueReporter.DEPTH, FlexMetricTags.of("queue", queue, "endpoint", endpoint), value);
    }
}
