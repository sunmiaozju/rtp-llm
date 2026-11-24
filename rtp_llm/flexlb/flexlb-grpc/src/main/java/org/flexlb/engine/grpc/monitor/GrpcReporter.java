package org.flexlb.engine.grpc.monitor;

import org.flexlb.enums.FlexMetricType;
import org.flexlb.enums.FlexPriorityType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ObjDoubleConsumer;

import static org.flexlb.constant.MetricConstant.GRPC_CALL_COUNT;
import static org.flexlb.constant.MetricConstant.GRPC_CALL_DURATION;
import static org.flexlb.constant.MetricConstant.GRPC_CHANNEL_POOL_SIZE;
import static org.flexlb.constant.MetricConstant.GRPC_CONNECTION_DURATION;
import static org.flexlb.constant.MetricConstant.GRPC_RESPONSE_SIZE;

/**
 * Reporter for gRPC channel pool metrics.
 *
 * Note: All gRPC metrics use "engineIp" as the IP tag name (previously "ip"),
 * consistent with other FlexLB reporters (EngineHealthReporter, BatchSchedulerReporter,
 * CacheMetricsReporter, etc.). If any Grafana dashboard queries reference the old "ip"
 * tag for these gRPC metrics, they must be updated to use "engineIp" instead.
 */
@Component
public class GrpcReporter {

    public static final String GET_WORKER_STATUS = "GetWorkerStatus";
    public static final String GET_CACHE_STATUS = "GetCacheStatus";
    public static final String ENQUEUE_BATCH = "EnqueueBatch";
    public static final String KVCM_GET_HOST_CACHE_STATE = "KVCM_GET_HOST_CACHE_STATE";

    private final FlexMonitor monitor;
    private final Map<String, CallReporters> callReporters = new HashMap<>();

    private record CallReporters(ObjDoubleConsumer<String> firstAttempt, ObjDoubleConsumer<String> retry) {
    }

    public GrpcReporter(FlexMonitor monitor) {
        this.monitor = monitor;
        for (String service : List.of(GET_WORKER_STATUS, GET_CACHE_STATUS, ENQUEUE_BATCH, KVCM_GET_HOST_CACHE_STATE)) {
            callReporters.put(service, new CallReporters(withCallTags(service, false), withCallTags(service, true)));
        }
    }

    @PostConstruct
    public void init() {
        this.monitor.register(GRPC_CHANNEL_POOL_SIZE, FlexMetricType.GAUGE);
        this.monitor.register(GRPC_CALL_DURATION, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        this.monitor.register(GRPC_RESPONSE_SIZE, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        this.monitor.register(GRPC_CALL_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        this.monitor.register(GRPC_CONNECTION_DURATION, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
    }

    /**
     * Report the current size of the gRPC channel pool
     *
     * @param channelPoolSize current number of channels in the pool
     */
    public void reportChannelPoolSize(int channelPoolSize) {
        monitor.report(GRPC_CHANNEL_POOL_SIZE, FlexMetricTags.of("type", "grpc"), channelPoolSize);
    }

    /**
     * Report gRPC call metrics
     *
     * @param serviceType Service type
     * @param duration Call duration in milliseconds
     * @param responseSize Response body size in bytes
     * @param retry Whether the call was retried
     */
    public void reportCallMetrics(String serviceType, long duration, int responseSize, boolean retry) {
        CallReporters reporters = callReporters.get(serviceType);
        ObjDoubleConsumer<String> report = reporters == null
                ? withCallTags(serviceType, retry)
                : (retry ? reporters.retry() : reporters.firstAttempt());
        report.accept(GRPC_CALL_DURATION, duration);
        report.accept(GRPC_RESPONSE_SIZE, responseSize);
        report.accept(GRPC_CALL_COUNT, 1);
    }

    private ObjDoubleConsumer<String> withCallTags(String service, boolean retry) {
        return monitor.withTags(FlexMetricTags.of("service", service, "retry", String.valueOf(retry)));
    }

    /**
     * Report gRPC connection duration
     *
     * @param serviceType Service type
     * @param connectionDuration Connection duration in microseconds
     */
    public void reportConnectionDuration(String serviceType, long connectionDuration) {
        FlexMetricTags tags = FlexMetricTags.of(
            "service", serviceType
        );

        // Report connection duration
        monitor.report(GRPC_CONNECTION_DURATION, tags, connectionDuration);
    }
}
