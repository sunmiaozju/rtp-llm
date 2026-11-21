package org.flexlb.service.monitor;

import org.flexlb.balance.scheduler.RequestState;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.request.QueueProgress;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestTable;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.dao.BalanceMetrics;
import org.flexlb.dao.BalanceMetrics.PrefillRouteMetrics;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.enums.DecodeTaskPhase;
import org.flexlb.enums.FlexMetricType;
import org.flexlb.enums.FlexPriorityType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.flexlb.constant.MetricConstant.AUTO_TPM_VICTIM_COUNT;
import static org.flexlb.constant.MetricConstant.CACHE_AFFINITY_DECISION;
import static org.flexlb.constant.MetricConstant.CACHE_AFFINITY_OVER_CAP_MS;
import static org.flexlb.constant.MetricConstant.CACHE_ROUTING_CANDIDATE_MAX_HIT_TOKENS;
import static org.flexlb.constant.MetricConstant.CACHE_ROUTING_SELECTED_MATCH_HIT_TOKENS;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_PREFILL_INFLIGHT;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_PREFILL_INFLIGHT_LIMIT;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_QUEUE_BLOCKED_ATTEMPT_QPS;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_QUEUE_BLOCKED_QPS;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_QUEUE_DEPTH;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_QUEUE_OLDEST_WAIT_MS;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_QUEUE_REJECTED_QPS;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_QUEUE_WAIT_MS;
import static org.flexlb.constant.MetricConstant.SCHEDULE_V2_REQUEST_PREPARE_SUBMIT_REJECTED_QPS;

/**
 * V2 的实际等待人数与出队检查结果。人数按请求阶段采样，不把共享名额当作 Prefill 队列长度。
 * blocked.qps 统计新进入阻塞原因的请求，blocked_attempt.qps 统计每次未通过的检查。
 */
@Component
public class V2QueueReporter {

    private Set<String> previousPrefillEndpoints = Set.of();
    private final FlexMonitor monitor;
    private final AsyncRequestMetrics asyncRequestMetrics;
    private Set<QueueKey> previousQueues = Set.of();

    public V2QueueReporter(FlexMonitor monitor, AsyncRequestMetrics asyncRequestMetrics) {
        this.monitor = monitor;
        this.asyncRequestMetrics = asyncRequestMetrics;
    }

    @PostConstruct
    public void init() {
        monitor.register(SCHEDULE_V2_QUEUE_DEPTH, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_QUEUE_OLDEST_WAIT_MS, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_PREFILL_INFLIGHT, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_PREFILL_INFLIGHT_LIMIT, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_QUEUE_WAIT_MS, FlexMetricType.TIMER, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_QUEUE_BLOCKED_QPS, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_QUEUE_BLOCKED_ATTEMPT_QPS, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_QUEUE_REJECTED_QPS, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(SCHEDULE_V2_REQUEST_PREPARE_SUBMIT_REJECTED_QPS, FlexMetricType.QPS, FlexPriorityType.NORMAL);
    }

    public void blocked(Request request, Stage stage, QueueBlockReason reason, String endpoint) {
        if (request.stage() != stage) {
            return;
        }
        BalanceMetrics metrics = request.ctx().getMetrics();
        var key = new BalanceMetrics.QueueBlockMetricKey(queueName(stage), reason.name());
        var counts = metrics.getBlockedCounts().computeIfAbsent(key, ignored -> new BalanceMetrics.BlockCounts());
        counts.getAttemptCount().incrementAndGet();
        if (request.recordBlock(stage, reason.name(), endpoint)) {
            counts.getDeduplicatedBlockCount().incrementAndGet();
        }
    }

    /**
     * 高优先级请求使 Prefill 等待请求退回主队列时，每次成功迁移上报一次。
     * 被退回请求继续排队，不计为拒绝或请求失败。
     */
    public void preemptedReturn(int preemptorPriority, int victimPriority, String endpoint) {
        reportPreemption("prefill_return_to_main", "prefill", "main", "requeue", preemptorPriority, victimPriority,
                endpoint);
    }

    public void mainPreempted(int preemptorPriority, int victimPriority) {
        reportPreemption("main_evicted", "main", "none", "reject", preemptorPriority, victimPriority, "all");
    }

    /**
     * 成功返回 Decode 抢占指令后，按每个受害者在选中时的阶段计数，不代表引擎已确认取消。
     * dispatched_unconfirmed 表示已开始下发，但 WorkerStatus 尚未确认引擎接收，不能归为运行中。
     */
    public void decodePreempted(int preemptorPriority, int victimPriority, DecodeTaskPhase phase, String endpoint) {
        String scenario = switch (phase) {
            case RUNNING -> "decode_running";
            case ACCEPTED_NOT_RUNNING -> "decode_accepted";
            case ENGINE_MAY_HAVE_SEEN -> "decode_dispatched_unconfirmed";
            case MASTER_QUEUED_NOT_DISPATCHED -> throw new IllegalArgumentException("victim is still in master queue");
        };
        reportPreemption(scenario, "decode_engine", "none", "return_cancel_instruction", preemptorPriority,
                victimPriority, endpoint);
    }

    private void reportPreemption(String scenario,
                                  String fromQueue,
                                  String toQueue,
                                  String action,
                                  int preemptorPriority,
                                  int victimPriority,
                                  String endpoint) {
        report(AUTO_TPM_VICTIM_COUNT,
                FlexMetricTags.of("case", scenario, "stage", fromQueue, "from_queue", fromQueue, "to_queue", toQueue,
                        "action", action, "incoming_priority", String.valueOf(preemptorPriority), "victim_priority",
                        String.valueOf(victimPriority)),
                1L);
    }

    /**
     * 请求完成后统一提交指标，不在完成线程调用监控后端。
     */
    public void completed(Request request, Response response) {
        QueueProgress progress = request.queueProgress();
        int priority = request.priority();
        String reason = rejectionReason(request, response, progress.stage());
        BalanceMetrics metrics = request.ctx().getMetrics();
        boolean succeeded = response != null && response.isSuccess();
        MetricInput input = new MetricInput(progress, priority, reason, metrics, succeeded,
                metrics.getPrepareSubmitRejectedCount());
        asyncRequestMetrics.submit(() -> reportCompletion(input));
    }

    private record MetricInput(QueueProgress progress,
                               int priority,
                               String rejectionReason,
                               BalanceMetrics metrics,
                               boolean succeeded,
                               int prepareSubmitRejectedCount) {
    }

    private static String rejectionReason(Request request, Response response, Stage stage) {
        if (!stage.isEnded() && (response == null || response.isSuccess())) {
            return null;
        }
        if (stage == Stage.CANCELLED && request.toRequestState().state() == RequestState.Phase.TIMED_OUT) {
            return "REQUEST_TIMEOUT";
        }
        if (response != null) {
            for (StrategyErrorType error : StrategyErrorType.values()) {
                if (error.getErrorCode() == response.getCode()) {
                    return error.name();
                }
            }
        }
        return stage == Stage.CANCELLED ? "REQUEST_CANCELLED" : "UNKNOWN";
    }

    private void reportCompletion(MetricInput input) {
        reportQueueCompletion(input);
        reportPrepareSubmitRejected(input);
        if (input.succeeded()) {
            PrefillRouteMetrics sample = input.metrics().getPrefillRouteMetrics();
            if (sample != null) {
                reportPrefillRouteMetrics(sample);
            }
        }
    }

    private void reportQueueCompletion(MetricInput input) {
        QueueProgress progress = input.progress();
        int priority = input.priority();
        String reason = input.rejectionReason();
        reportBlockCounts(input.metrics(), priority);
        if (reason != null) {
            report(SCHEDULE_V2_QUEUE_REJECTED_QPS, blockTags(queueName(progress.previousStage()), reason, priority), 1L);
        }
        String result = progress.stage() == Stage.PUBLISHED ? "success" : "failed";
        report(SCHEDULE_V2_QUEUE_WAIT_MS, FlexMetricTags.of("queue", "main", "result", result), progress.mainMs());
        report(SCHEDULE_V2_QUEUE_WAIT_MS, FlexMetricTags.of("queue", "prefill", "result", result), progress.prefillMs());
        report(SCHEDULE_V2_QUEUE_WAIT_MS, FlexMetricTags.of("queue", "decode", "result", result), progress.decodeMs());
    }

    private void reportPrepareSubmitRejected(MetricInput input) {
        if (input.prepareSubmitRejectedCount() > 0) {
            report(SCHEDULE_V2_REQUEST_PREPARE_SUBMIT_REJECTED_QPS,
                    FlexMetricTags.of("priority", String.valueOf(input.priority())),
                    input.prepareSubmitRejectedCount());
        }
    }

    private void reportPrefillRouteMetrics(PrefillRouteMetrics sample) {
        FlexMetricTags tags = FlexMetricTags.of();
        report(CACHE_ROUTING_SELECTED_MATCH_HIT_TOKENS, tags, sample.selectedHitTokens());
        report(CACHE_ROUTING_CANDIDATE_MAX_HIT_TOKENS, tags, sample.candidateMaxHitTokens());
        if (sample.affinityDecision() != null) {
            report(CACHE_AFFINITY_DECISION, FlexMetricTags.of("decision", sample.affinityDecision()), 1L);
        }
        if (sample.overCapMs() != null) {
            report(CACHE_AFFINITY_OVER_CAP_MS, tags, sample.overCapMs());
        }
    }

    private void reportBlockCounts(BalanceMetrics metrics, int priority) {
        metrics.getBlockedCounts().forEach((key, counts) -> {
            long blocks = counts.getDeduplicatedBlockCount().get();
            long attempts = counts.getAttemptCount().get();
            FlexMetricTags tags = blockTags(key.queue(), key.reason(), priority);
            if (attempts > 0L) {
                report(SCHEDULE_V2_QUEUE_BLOCKED_ATTEMPT_QPS, tags, attempts);
            }
            if (blocks > 0L) {
                report(SCHEDULE_V2_QUEUE_BLOCKED_QPS, tags, blocks);
            }
        });
    }

    /**
     * 每秒读取一次阶段，过渡中的请求只归属一个队列。已发布请求不计入等待。
     * 空队列继续报零，节点移除后的下一轮补零，避免大盘保留旧值。
     */
    public void sample(RequestTable requests, List<String> prefillEndpoints) {
        Map<QueueKey, Long> counts = new HashMap<>();
        Map<QueueKey, Long> oldest = new HashMap<>();
        for (Stage stage : new Stage[]{Stage.MAIN_WAITING, Stage.DECODE_WAITING, Stage.DECODE_RESERVED}) {
            counts.put(new QueueKey(queueName(stage), "all"), 0L);
        }
        for (String endpoint : prefillEndpoints) {
            counts.put(new QueueKey("prefill", endpoint), 0L);
        }
        long now = System.nanoTime();
        for (Request request : requests.list()) {
            QueueProgress progress = request.queueProgress();
            Stage stage = progress.stage();
            if (stage.isEnded() || stage == Stage.PUBLISHED) {
                continue;
            }
            var choice = request.selectedPrefill();
            String endpoint = stage == Stage.PREFILL_WAITING && choice != null ? choice.endpoint().ipPort() : "all";
            QueueKey key = new QueueKey(queueName(stage), endpoint);
            counts.merge(key, 1L, Long::sum);
            long ageMs =
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - progress.enteredAtNanos()));
            oldest.merge(key, ageMs, Math::max);
        }
        Set<QueueKey> keys = new HashSet<>(previousQueues);
        keys.addAll(counts.keySet());
        for (QueueKey key : keys) {
            FlexMetricTags tags = FlexMetricTags.of("queue", key.queue(), "endpoint", key.endpoint());
            report(SCHEDULE_V2_QUEUE_DEPTH, tags, counts.getOrDefault(key, 0L));
            report(SCHEDULE_V2_QUEUE_OLDEST_WAIT_MS, tags, oldest.getOrDefault(key, 0L));
        }
        keys.retainAll(counts.keySet());
        previousQueues = keys;
    }

    /**
     * 每秒采样准入占用数和当前配置上限。移除的端点补零，避免保留旧占用。
     */
    public synchronized void samplePrefillInflight(List<PrefillQueue> queues, int limit) {
        Set<String> endpoints = new HashSet<>();
        for (PrefillQueue queue : queues) {
            String endpoint = queue.endpoint().ipPort();
            endpoints.add(endpoint);
            FlexMetricTags tags = FlexMetricTags.of("endpoint", endpoint);
            report(SCHEDULE_V2_PREFILL_INFLIGHT, tags, queue.inflightCount());
        }
        for (String endpoint : previousPrefillEndpoints) {
            if (!endpoints.contains(endpoint)) {
                FlexMetricTags tags = FlexMetricTags.of("endpoint", endpoint);
                report(SCHEDULE_V2_PREFILL_INFLIGHT, tags, 0L);
            }
        }
        report(SCHEDULE_V2_PREFILL_INFLIGHT_LIMIT, FlexMetricTags.of(), limit);
        previousPrefillEndpoints = endpoints;
    }

    public static String queueName(Stage stage) {
        return switch (stage) {
            case MAIN_WAITING -> "main";
            case PREFILL_WAITING -> "prefill";
            case DECODE_WAITING -> "decode_waiting";
            case DECODE_RESERVED -> "decode_reserved";
            default -> "none";
        };
    }

    private static FlexMetricTags blockTags(String queue, String reason, int priority) {
        return FlexMetricTags.of("queue", queue, "reason", reason, "priority", String.valueOf(priority));
    }

    private void report(String metric, FlexMetricTags tags, long value) {
        try {
            monitor.report(metric, tags, value);
        } catch (Throwable failure) {
            Logger.warn("V2 queue metric report failed: metric={}", metric, failure);
        }
    }

    private record QueueKey(String queue, String endpoint) {
    }
}
