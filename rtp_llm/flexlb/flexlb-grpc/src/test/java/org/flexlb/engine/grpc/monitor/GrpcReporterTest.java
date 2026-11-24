package org.flexlb.engine.grpc.monitor;

import org.flexlb.constant.MetricConstant;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcReporterTest {

    @Test
    void reusesServiceAndRetryTagsAcrossCallSamples() {
        FlexMonitor monitor = mock(FlexMonitor.class);
        when(monitor.withTags(any(FlexMetricTags.class))).thenCallRealMethod();
        GrpcReporter reporter = new GrpcReporter(monitor);

        reporter.reportCallMetrics(GrpcReporter.GET_WORKER_STATUS, 2, 128, false);
        reporter.reportCallMetrics(GrpcReporter.GET_WORKER_STATUS, 3, 256, false);
        reporter.reportCallMetrics(GrpcReporter.GET_WORKER_STATUS, 5, 64, true);

        FlexMetricTags firstAttemptTags = FlexMetricTags.of("service", "GetWorkerStatus", "retry", "false");
        FlexMetricTags retryTags = FlexMetricTags.of("service", "GetWorkerStatus", "retry", "true");
        verify(monitor).withTags(firstAttemptTags);
        verify(monitor).withTags(retryTags);
        verify(monitor).report(MetricConstant.GRPC_CALL_DURATION, firstAttemptTags, 2.0);
        verify(monitor).report(MetricConstant.GRPC_CALL_DURATION, retryTags, 5.0);
        verify(monitor).report(MetricConstant.GRPC_RESPONSE_SIZE, firstAttemptTags, 256.0);
        verify(monitor).report(MetricConstant.GRPC_RESPONSE_SIZE, retryTags, 64.0);
        verify(monitor, times(2)).report(MetricConstant.GRPC_CALL_COUNT, firstAttemptTags, 1.0);
        verify(monitor).report(MetricConstant.GRPC_CALL_COUNT, retryTags, 1.0);
    }
}
