package org.flexlb.service.monitor;

import org.flexlb.dao.master.WorkerStatus.StepMetrics;
import org.flexlb.dao.route.RoleType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_BUDGET_FILL_RATIO;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerStatusMetricsReporterTest {

    private final FlexMonitor monitor = mock(FlexMonitor.class);
    private final WorkerStatusMetricsReporter reporter = new WorkerStatusMetricsReporter(monitor);
    private final FlexMetricTags tags = FlexMetricTags.of("role", "DECODE", "group", "group", "phase", "decode");

    @Test
    void averagesSamplesFromMultipleThreadsAndReportsOnlyNewValuesInTheNextWindow() throws Exception {
        when(monitor.withTags(any(FlexMetricTags.class))).thenCallRealMethod();
        try (var firstWriter = Executors.newSingleThreadExecutor();
                var secondWriter = Executors.newSingleThreadExecutor()) {
            var first = firstWriter.submit(() -> recordStep(100, 0.25));
            var second = secondWriter.submit(() -> {
                recordStep(300, 0.75);
                recordStep(300, 0.75);
                recordStep(300, 0.75);
            });
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            reporter.report();

            firstWriter.submit(() -> recordStep(200, 0.5)).get(5, TimeUnit.SECONDS);
            secondWriter.submit(() -> recordStep(400, Double.POSITIVE_INFINITY)).get(5, TimeUnit.SECONDS);
            reporter.report();
            reporter.report();
        }

        verify(monitor).withTags(tags);
        verify(monitor).report(ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS, tags, 250.0);
        verify(monitor).report(ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS, tags, 300.0);
        verify(monitor).report(ENGINE_WORKER_STEP_BUDGET_FILL_RATIO, tags, 0.625);
        verify(monitor).report(ENGINE_WORKER_STEP_BUDGET_FILL_RATIO, tags, 0.5);
    }

    @Test
    void blockedReportingDoesNotBlockRecordingIntoTheNextWindow() throws Exception {
        when(monitor.withTags(any(FlexMetricTags.class))).thenCallRealMethod();
        var reporting = new CountDownLatch(1);
        var finishReport = new CountDownLatch(1);
        doAnswer(invocation -> {
            reporting.countDown();
            assertTrue(finishReport.await(10, TimeUnit.SECONDS));
            return null;
        }).doNothing().when(monitor).report(eq(ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS), eq(tags), anyDouble());

        recordStep(120, 0.25);
        try (var threads = Executors.newFixedThreadPool(2)) {
            var report = threads.submit(reporter::report);
            try {
                assertTrue(reporting.await(5, TimeUnit.SECONDS));
                threads.submit(() -> recordStep(200, 0.5)).get(5, TimeUnit.SECONDS);
                assertFalse(report.isDone());
            } finally {
                finishReport.countDown();
            }
            report.get(5, TimeUnit.SECONDS);
        }

        reporter.report();
        verify(monitor).report(ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS, tags, 120.0);
        verify(monitor).report(ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS, tags, 200.0);
        verify(monitor).report(ENGINE_WORKER_STEP_BUDGET_FILL_RATIO, tags, 0.25);
        verify(monitor).report(ENGINE_WORKER_STEP_BUDGET_FILL_RATIO, tags, 0.5);
    }

    private void recordStep(long tokens, double fillRatio) {
        reporter.recordStep(RoleType.DECODE, "group", new StepMetrics(1, 1, tokens, 0, 0, 400, fillRatio));
    }
}
