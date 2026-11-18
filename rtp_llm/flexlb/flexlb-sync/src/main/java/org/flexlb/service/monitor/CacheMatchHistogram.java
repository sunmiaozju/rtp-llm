package org.flexlb.service.monitor;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.flexlb.constant.MetricConstant.CACHE_FIND_MATCHING_ENGINES_RT;

@Component
public class CacheMatchHistogram {

    private static final List<Long> BUCKETS_US =
            List.of(0L, 125L, 250L, 500L, 750L, 1000L, 1500L, 2000L, 2500L, 3000L, 3500L, 4000L, 4500L, 5000L, 6000L,
                    7000L, 8000L, 9000L, 10000L, 12000L, 15000L, 20000L, 30000L, 50000L, 100000L, 250000L, 1000000L);

    private final SdkMeterProvider provider;
    private final LongHistogram histogram;
    private final Attributes tags;

    @Autowired
    public CacheMatchHistogram(@Value("${FLEXLB_CACHE_MATCH_OTLP_ENDPOINT:}") String endpoint) {
        this(createReader(endpoint), System.getenv().getOrDefault("SPECTRUM_APPLICATION_NAME", "whale-lb"),
                System.getenv().getOrDefault("HOSTNAME", ""),
                Attributes.builder()
                        .put("biz_name", System.getenv().getOrDefault("BIZ_NAME", ""))
                        .put("hippo_role", System.getenv().getOrDefault("HIPPO_ROLE", ""))
                        .put("ip", System.getenv().getOrDefault("POD_IP", ""))
                        .build());
    }

    CacheMatchHistogram(MetricReader reader, String service, String hostname, Attributes tags) {
        this.tags = tags;
        if (reader == null) {
            provider = null;
            histogram = null;
            return;
        }
        provider = SdkMeterProvider.builder()
                .registerMetricReader(reader)
                .setResource(Resource.getDefault()
                        .merge(Resource.create(
                                Attributes.of(stringKey("service.name"), service, stringKey("host.name"), hostname))))
                .build();
        histogram = provider.get("flexlb.cache-match")
                .histogramBuilder(CACHE_FIND_MATCHING_ENGINES_RT)
                .ofLongs()
                .setUnit("us")
                .setExplicitBucketBoundariesAdvice(BUCKETS_US)
                .build();
        Logger.info("Cache match OTLP histogram enabled");
    }

    void record(long cacheMatchUs) {
        if (histogram != null) {
            histogram.record(cacheMatchUs, tags);
        }
    }

    @PreDestroy
    void close() {
        if (provider != null) {
            provider.shutdown().join(3, TimeUnit.SECONDS);
        }
    }

    private static MetricReader createReader(String endpoint) {
        if (endpoint.isBlank()) {
            return null;
        }
        try {
            OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
                    .setEndpoint(endpoint.trim())
                    .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
                    .setTimeout(Duration.ofSeconds(3))
                    .build();
            return PeriodicMetricReader.builder(exporter).setInterval(Duration.ofSeconds(20)).build();
        } catch (IllegalArgumentException invalidEndpoint) {
            Logger.warn("Cache match OTLP histogram disabled: invalid metrics endpoint");
            return null;
        }
    }
}
