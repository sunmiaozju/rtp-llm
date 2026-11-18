package org.flexlb.service.monitor;

import io.micrometer.core.instrument.util.NamedThreadFactory;
import org.flexlb.enums.FlexMetricType;
import org.flexlb.enums.FlexPriorityType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.flexlb.util.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.flexlb.constant.MetricConstant.CACHE_FIND_MATCHING_ENGINES_RT;

@Component
public class AsyncRequestMetrics {

    static final String DROPPED_REPORTS = "schedule_v2.metrics_report.dropped";
    private static final int REPORT_QUEUE_CAPACITY = 40960;
    private static final int REPORT_THREADS = Runtime.getRuntime().availableProcessors();

    private final AtomicLong reportsDroppedQueueFull = new AtomicLong();
    private final ThreadPoolExecutor reportExecutor = new ThreadPoolExecutor(REPORT_THREADS, REPORT_THREADS * 3, 60L,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(REPORT_QUEUE_CAPACITY),
            new NamedThreadFactory("flexlb-request-metrics-reporter"), new ThreadPoolExecutor.AbortPolicy());
    private final FlexMonitor monitor;
    private final CacheMatchHistogram cacheMatchHistogram;

    public AsyncRequestMetrics(FlexMonitor monitor, CacheMatchHistogram cacheMatchHistogram) {
        this.monitor = monitor;
        this.cacheMatchHistogram = cacheMatchHistogram;
    }

    @PostConstruct
    void init() {
        monitor.register(DROPPED_REPORTS, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
    }

    public void reportCacheMatch(long cacheMatchUs) {
        submit(() -> {
            report(CACHE_FIND_MATCHING_ENGINES_RT, FlexMetricTags.of(), cacheMatchUs);
            cacheMatchHistogram.record(cacheMatchUs);
        });
    }

    void submit(Runnable reportTask) {
        try {
            reportExecutor.execute(reportTask);
        } catch (RejectedExecutionException rejected) {
            if (!reportExecutor.isShutdown()) {
                reportsDroppedQueueFull.incrementAndGet();
            }
        }
    }

    @Scheduled(fixedRate = 20_000L, initialDelay = 20_000L)
    void reportDroppedQueueFull() {
        report(DROPPED_REPORTS, FlexMetricTags.of("reason", "queue_full"), reportsDroppedQueueFull.getAndSet(0L));
    }

    @PreDestroy
    void closeReportExecutor() {
        reportExecutor.shutdown();
    }

    private void report(String metric, FlexMetricTags tags, long value) {
        try {
            monitor.report(metric, tags, value);
        } catch (Throwable failure) {
            Logger.warn("Async request metric report failed: metric={}", metric, failure);
        }
    }
}
