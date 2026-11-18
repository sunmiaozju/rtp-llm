package org.flexlb.service.monitor;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;

import static org.flexlb.constant.MetricConstant.CACHE_FIND_MATCHING_ENGINES_RT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheMatchHistogramTest {

    @Test
    void recordsZeroAndRawMicrosecondsInDeltaHistogram() {
        MetricReader reader = mock(MetricReader.class, CALLS_REAL_METHODS);
        when(reader.getAggregationTemporality(any())).thenReturn(AggregationTemporality.DELTA);
        when(reader.shutdown()).thenReturn(CompletableResultCode.ofSuccess());
        Attributes tags = Attributes.builder().put("biz_name", "test-biz").put("hippo_role", "test-master").build();
        CacheMatchHistogram histogram = new CacheMatchHistogram(reader, "whale-lb", "test-host", tags);
        try {
            ArgumentCaptor<CollectionRegistration> registration = ArgumentCaptor.forClass(CollectionRegistration.class);
            verify(reader).register(registration.capture());
            assertTrue(registration.getValue().collectAllMetrics().isEmpty());

            histogram.record(0);
            histogram.record(3200);
            Collection<MetricData> metrics = registration.getValue().collectAllMetrics();
            assertEquals(1, metrics.size());
            MetricData metric = metrics.iterator().next();
            assertEquals(CACHE_FIND_MATCHING_ENGINES_RT, metric.getName());
            assertEquals("us", metric.getUnit());
            assertEquals(AggregationTemporality.DELTA, metric.getHistogramData().getAggregationTemporality());
            var point = metric.getHistogramData().getPoints().iterator().next();
            assertEquals(tags, point.getAttributes());
            assertEquals(2, point.getCount());
            assertEquals(3200, point.getSum());
            assertEquals(0.0, point.getBoundaries().getFirst());
            assertEquals(1L, point.getCounts().getFirst());
            assertTrue(registration.getValue().collectAllMetrics().isEmpty());
        } finally {
            histogram.close();
        }
    }

    @Test
    void asyncReportSendsSameSampleToBothBackends() {
        FlexMonitor monitor = mock(FlexMonitor.class);
        CacheMatchHistogram histogram = mock(CacheMatchHistogram.class);
        AsyncRequestMetrics metrics = new AsyncRequestMetrics(monitor, histogram);
        try {
            metrics.reportCacheMatch(0);
            verify(monitor, timeout(2000).times(1)).report(CACHE_FIND_MATCHING_ENGINES_RT, FlexMetricTags.of(), 0.0);
            verify(histogram, timeout(2000).times(1)).record(0);
        } finally {
            metrics.closeReportExecutor();
        }
    }
}
