package org.flexlb.service.monitor;

import lombok.extern.slf4j.Slf4j;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.FlexMetricType;
import org.flexlb.enums.FlexPriorityType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.flexlb.util.PriorityNormalizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static org.flexlb.constant.MetricConstant.AUTO_TPM_CANCEL_QPS;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_EVICTION_COMMIT_COUNT;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_EVICTION_PLAN_COUNT;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_PREEMPTION_TARGET_INVALID_COUNT;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_PREFILL_QUEUE_DEPTH;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_PRIORITY_PREEMPT_COUNT;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_REQUEST_COUNT;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_TTFT_MS;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_VICTIM_COUNT;
import static org.flexlb.constant.MetricConstant.AUTO_TPM_VICTIM_KV_TOKENS;
import static org.flexlb.constant.MetricConstant.REQUEST_LIFECYCLE_FAILURES_TOTAL;
import static org.flexlb.constant.MetricConstant.REQUEST_STATUS_UPDATE_TIMEOUT_COUNT;

/**
 * Auto-TPM priority scheduling metrics reporter.
 *
 * <p>Observability for per-priority request volume, schedule latency,
 * placement, preemption, cancellation, and resource ownership.
 */
@Slf4j
@Component
public class RequestSchedulerReporter {

    /**
     * Priority is a normalized level, so its single-tag set is shared per
     * level instead of being rebuilt on every report.
     */
    private static final FlexMetricTags[] PRIORITY_TAGS = buildPriorityTags();

    private final FlexMonitor monitor;

    @Autowired
    public RequestSchedulerReporter(FlexMonitor monitor) {
        this.monitor = monitor;
    }

    private static FlexMetricTags[] buildPriorityTags() {
        FlexMetricTags[] tags = new FlexMetricTags[
                PriorityNormalizer.MAX_PRIORITY + 1];
        for (int priority = 0; priority < tags.length; priority++) {
            tags[priority] = FlexMetricTags.of("priority", String.valueOf(priority));
        }
        return tags;
    }

    private static FlexMetricTags priorityTags(int priority) {
        return priority >= 0 && priority <= PriorityNormalizer.MAX_PRIORITY
                ? PRIORITY_TAGS[priority]
                : FlexMetricTags.of("priority", String.valueOf(priority));
    }

    @PostConstruct
    public void init() {
        monitor.register(AUTO_TPM_REQUEST_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_EVICTION_PLAN_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_EVICTION_COMMIT_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_VICTIM_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_PREFILL_QUEUE_DEPTH, FlexMetricType.GAUGE, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_TTFT_MS, FlexMetricType.TIMER, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_PRIORITY_PREEMPT_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_CANCEL_QPS, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_PREEMPTION_TARGET_INVALID_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        monitor.register(REQUEST_LIFECYCLE_FAILURES_TOTAL, FlexMetricType.COUNTER, FlexPriorityType.NORMAL);
        monitor.register(AUTO_TPM_VICTIM_KV_TOKENS, FlexMetricType.TIMER, FlexPriorityType.NORMAL);
        monitor.register(REQUEST_STATUS_UPDATE_TIMEOUT_COUNT, FlexMetricType.QPS, FlexPriorityType.NORMAL);
        reportLifecycleFailureCounts();
        log.info("RequestSchedulerReporter initialized");
    }

    /**
     * Report request arrival by scheduling phase and normalized priority.
     */
    public void reportRequest(BalanceContext context) {
        monitor.report(AUTO_TPM_REQUEST_COUNT, FlexMetricTags.of("phase", context.getSchedulePhase(), "priority",
                String.valueOf(context.getPriority())), 1.0);
    }

    /**
     * Report one Decode preemption attempt rejected by target validation.
     *
     * @param mode return or rpc
     * @param reason fixed target-validation reason; never a request ID or error message
     */
    public void reportPreemptionTargetInvalid(String mode, String reason) {
        monitor.report(AUTO_TPM_PREEMPTION_TARGET_INVALID_COUNT,
                FlexMetricTags.of("mode", mode, "reason", reason), 1.0);
    }

    /**
     * Report one request released locally because its first Engine status never arrived after delivery.
     */
    public void reportStatusUpdateTimeout() {
        monitor.report(REQUEST_STATUS_UPDATE_TIMEOUT_COUNT, FlexMetricTags.of(), 1.0);
    }

    /**
     * 按角色和端点记录请求状态更新超时，包括首次上报超时和已上报后持续静默。
     */
    public void reportStatusUpdateTimeout(RoleType role, String endpoint) {
        monitor.report(REQUEST_STATUS_UPDATE_TIMEOUT_COUNT,
                FlexMetricTags.of("role", role.name(), "endpoint", endpoint), 1.0);
    }

    /**
     * Report a registration or terminal_cleanup failure without interrupting request cleanup.
     */
    public void reportLifecycleFailure(String stage) {
        reportLifecycleFailureCount(stage, 1.0);
    }

    @Scheduled(fixedRate = 20000)
    private void reportLifecycleFailureCounts() {
        reportLifecycleFailureCount("registration", 0.0);
        reportLifecycleFailureCount("terminal_cleanup", 0.0);
    }

    private void reportLifecycleFailureCount(String stage, double count) {
        try {
            monitor.report(REQUEST_LIFECYCLE_FAILURES_TOTAL, FlexMetricTags.of("stage", stage), count);
        } catch (Throwable failure) {
            log.warn("Failed to report request lifecycle failure, stage={}", stage, failure);
        }
    }

    /**
     * Report an eviction plan generation outcome via
     * {@code auto_tpm.eviction_plan.count}.
     *
     * @param priority incoming request priority
     * @param evCase   eviction case label (e.g. decode_kv_full)
     * @param result   plan result label (feasible / infeasible)
     */
    public void reportEvictionPlan(int priority, String evCase, String result) {
        monitor.report(AUTO_TPM_EVICTION_PLAN_COUNT,
                FlexMetricTags.of("priority", String.valueOf(priority), "case", evCase, "result", result), 1.0);
    }

    /**
     * Report an eviction plan commit outcome via
     * {@code auto_tpm.eviction_commit.count}.
     *
     * @param priority incoming request priority
     * @param evCase   eviction case label (e.g. decode_kv_full)
     * @param result   commit result label (success / version_mismatch / partial_failure)
     */
    public void reportEvictionCommit(int priority, String evCase, String result) {
        monitor.report(AUTO_TPM_EVICTION_COMMIT_COUNT,
                FlexMetricTags.of("priority", String.valueOf(priority), "case", evCase, "result", result), 1.0);
    }

    /**
     * Report one evicted victim via {@code auto_tpm.victim.count}.
     *
     * @param victimPriority   priority of the evicted request
     * @param incomingPriority priority of the request that displaced it
     * @param stage            victim scheduling stage (e.g. prefill_queued)
     * @param evCase           eviction case label (e.g. decode_kv_full)
     */
    public void reportVictim(int victimPriority, int incomingPriority, String stage, String evCase) {
        monitor.report(AUTO_TPM_VICTIM_COUNT,
                FlexMetricTags.of("victim_priority", String.valueOf(victimPriority),
                        "incoming_priority", String.valueOf(incomingPriority),
                        "stage", stage, "case", evCase), 1.0);
    }

    /**
     * Report a prefill batcher queue depth via
     * {@code auto_tpm.prefill.queue_depth}.
     */
    public void reportPrefillQueueDepth(String endpoint, int depth) {
        monitor.report(AUTO_TPM_PREFILL_QUEUE_DEPTH,
                FlexMetricTags.of("endpoint", endpoint), depth);
    }

    /**
     * Report one victim's released hard KV tokens via
     * {@code auto_tpm.victim.kv_tokens} (design doc 19.2).
     *
     * @param victimPriority priority of the evicted request
     * @param stage          victim scheduling stage (e.g. decode_reserved)
     * @param kvTokens       hard KV tokens released by the eviction
     */
    public void reportVictimKvTokens(int victimPriority, String stage, long kvTokens) {
        monitor.report(AUTO_TPM_VICTIM_KV_TOKENS,
                FlexMetricTags.of("victim_priority", String.valueOf(victimPriority),
                        "stage", stage), kvTokens);
    }

    /**
     * Report the TTFT approximation via {@code auto_tpm.ttft_ms}.
     * Approximated as "request arrival → schedule completion" on the Master;
     * the engine-side first-token time is not observable here, so this is a
     * lower bound of the real TTFT.
     */
    public void reportTtft(int priority, long latencyMs) {
        monitor.report(AUTO_TPM_TTFT_MS,
                priorityTags(priority), latencyMs);
    }

    /**
     * Report one priority preemption via
     * {@code auto_tpm.priority_preempt.count}.
     *
     * @param stage victim scheduling stage (prefill_queued / decode_reserved /
     *              decode_running / decode_cancel)
     */
    public void reportPriorityPreempt(String stage) {
        monitor.report(AUTO_TPM_PRIORITY_PREEMPT_COUNT,
                FlexMetricTags.of("stage", stage), 1.0);
    }

    /**
     * Report one cancellation request, tagged with its priority and reason
     * (PRIORITY_PREEMPTED / USER_CANCELLED / DEADLINE_EXCEEDED).
     *
     * @param priority normalized priority of the cancelled request
     * @param reason   cancel reason metric label
     */
    public void reportCancel(int priority, String reason) {
        monitor.report(AUTO_TPM_CANCEL_QPS,
                FlexMetricTags.of("priority", String.valueOf(priority), "reason", reason), 1.0);
    }

}
