package org.flexlb.service.monitor;

import org.flexlb.dao.master.WorkerStatus.StepMetrics;
import org.flexlb.dao.master.WorkerStatus.TaskObservation;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.FlexMetricType;
import org.flexlb.enums.FlexPriorityType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ToDoubleFunction;

import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_ENGINE_OBSERVED_RECEIVED_TO_WAITING_MS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_ENGINE_OBSERVED_WAITING_TO_RUNNING_MS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_HBM_LOCAL_MATCH_TOKENS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_INPUT_QUEUE_WAIT_MS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_PREFILL_NONFINAL_CHUNK_TOKENS_MAX;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_PREFILL_NONFINAL_CHUNK_TOKENS_MIN;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_PREFILL_STEP_COUNT;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_REMOTE_KV_ADDED_MATCH_TOKENS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_REMOTE_KV_WAIT_MS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_RUNNING_TO_FIRST_TOKEN_MS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STATUS_SCHEDULER_WAIT_MS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_BUDGET_FILL_RATIO;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_PREFILL_REQUEST_COUNT;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_PREFILL_TOKENS;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_TOKEN_BUDGET;
import static org.flexlb.constant.MetricConstant.ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS;

/**
 * Collects per-task and per-step averages without retaining the observations.
 */
final class WorkerStatusMetricsReporter {

    private static final Object NO_GROUP = new Object();
    private static final List<Metric<TaskObservation>> TASK_METRICS =
            List.of(new Metric<>(ENGINE_WORKER_STATUS_HBM_LOCAL_MATCH_TOKENS, TaskObservation::hbmLocalMatchTokens),
                    new Metric<>(ENGINE_WORKER_STATUS_REMOTE_KV_ADDED_MATCH_TOKENS,
                            TaskObservation::remoteKvAddedMatchTokens),
                    new Metric<>(ENGINE_WORKER_STATUS_PREFILL_STEP_COUNT, TaskObservation::prefillStepCount),
                    new Metric<>(ENGINE_WORKER_STATUS_PREFILL_NONFINAL_CHUNK_TOKENS_MIN, FlexPriorityType.TRIVIAL,
                            task -> hasNonfinalChunk(task) ? task.prefillNonfinalChunkTokensMin() : Double.NaN),
                    new Metric<>(ENGINE_WORKER_STATUS_PREFILL_NONFINAL_CHUNK_TOKENS_MAX, FlexPriorityType.TRIVIAL,
                            task -> hasNonfinalChunk(task) ? task.prefillNonfinalChunkTokensMax() : Double.NaN),
                    new Metric<>(ENGINE_WORKER_STATUS_INPUT_QUEUE_WAIT_MS,
                            task -> duration(task.inputQueueDrainTimeMs(), task.inputQueueEnqueueTimeMs())),
                    new Metric<>(ENGINE_WORKER_STATUS_REMOTE_KV_WAIT_MS, TaskObservation::remoteKvWaitMs),
                    new Metric<>(ENGINE_WORKER_STATUS_ENGINE_OBSERVED_WAITING_TO_RUNNING_MS,
                            WorkerStatusMetricsReporter::waitingToRunning),
                    new Metric<>(ENGINE_WORKER_STATUS_SCHEDULER_WAIT_MS,
                            task -> Math.max(0, waitingToRunning(task) - task.remoteKvWaitMs())),
                    new Metric<>(ENGINE_WORKER_STATUS_RUNNING_TO_FIRST_TOKEN_MS,
                            task -> duration(task.firstTokenTimeMs(), task.runningEnteredTimeMs())),
                    new Metric<>(ENGINE_WORKER_STATUS_ENGINE_OBSERVED_RECEIVED_TO_WAITING_MS,
                            task -> duration(task.waitingEnteredTimeMs(), task.requestReceivedTimeMs())));
    private static final List<Metric<StepMetrics>> STEP_METRICS =
            List.of(new Metric<>(ENGINE_WORKER_STEP_TOTAL_SCHEDULED_TOKENS, StepMetrics::totalScheduledTokens),
                    new Metric<>(ENGINE_WORKER_STEP_PREFILL_REQUEST_COUNT, StepMetrics::prefillRequestCount),
                    new Metric<>(ENGINE_WORKER_STEP_PREFILL_TOKENS, StepMetrics::prefillTokens),
                    new Metric<>(ENGINE_WORKER_STEP_TOKEN_BUDGET, StepMetrics::tokenBudget),
                    new Metric<>(ENGINE_WORKER_STEP_BUDGET_FILL_RATIO, StepMetrics::budgetFillRatio));

    private final FlexMonitor monitor;
    private final Map<String, Map<Object, GroupMetrics>> groupsByRole = new ConcurrentHashMap<>();

    WorkerStatusMetricsReporter(FlexMonitor monitor) {
        this.monitor = monitor;
    }

    void register() {
        TASK_METRICS.forEach(metric -> monitor.register(metric.name(), FlexMetricType.GAUGE, metric.priority()));
        STEP_METRICS.forEach(metric -> monitor.register(metric.name(), FlexMetricType.GAUGE, metric.priority()));
    }

    void recordPrefillTasks(RoleType role, String group, Collection<TaskObservation> finishedTasks) {
        MetricWindow<TaskObservation> window = null;
        for (TaskObservation task : finishedTasks) {
            if (task.firstTokenTimeMs() <= 0) {
                continue;
            }
            if (window == null) {
                window = groupMetrics(role, group).finishedTasks();
            }
            window.add(task);
        }
    }

    void recordStep(RoleType role, String group, StepMetrics step) {
        GroupMetrics metrics = groupMetrics(role, group);
        MetricWindow<StepMetrics> window =
                step.prefillRequestCount() > 0 ? metrics.prefillSteps() : metrics.decodeSteps();
        window.add(step);
    }

    void report() {
        for (Map<Object, GroupMetrics> groups : groupsByRole.values()) {
            for (GroupMetrics metrics : groups.values()) {
                metrics.finishedTasks().report(monitor);
                metrics.prefillSteps().report(monitor);
                metrics.decodeSteps().report(monitor);
            }
        }
    }

    private GroupMetrics groupMetrics(RoleType role, String group) {
        String roleName = role == null ? "" : role.name();
        Map<Object, GroupMetrics> groups = groupsByRole.get(roleName);
        if (groups == null) {
            groups = groupsByRole.computeIfAbsent(roleName, ignored -> new ConcurrentHashMap<>());
        }
        Object groupKey = group == null ? NO_GROUP : group;
        GroupMetrics metrics = groups.get(groupKey);
        if (metrics == null) {
            metrics = groups.computeIfAbsent(groupKey,
                    ignored -> new GroupMetrics(
                            new MetricWindow<>(TASK_METRICS,
                                    FlexMetricTags.of("role", roleName, "group", group == null ? "" : group)),
                            new MetricWindow<>(STEP_METRICS,
                                    FlexMetricTags.of("role", roleName, "group", group, "phase", "prefill")),
                            new MetricWindow<>(STEP_METRICS,
                                    FlexMetricTags.of("role", roleName, "group", group, "phase", "decode"))));
        }
        return metrics;
    }

    private static boolean hasNonfinalChunk(TaskObservation task) {
        return task.prefillNonfinalChunkTokensMin() > 0 && task.prefillNonfinalChunkTokensMax() > 0;
    }

    private static double waitingToRunning(TaskObservation task) {
        return duration(task.runningEnteredTimeMs(), task.waitingEnteredTimeMs());
    }

    private static double duration(long endMs, long startMs) {
        return endMs > 0 && startMs > 0 ? Math.max(0L, endMs - startMs) : Double.NaN;
    }

    private record Metric<T>(String name, FlexPriorityType priority, ToDoubleFunction<T> value) {
        Metric(String name, ToDoubleFunction<T> value) {
            this(name, FlexPriorityType.NORMAL, value);
        }
    }

    private record GroupMetrics(MetricWindow<TaskObservation> finishedTasks,
                                MetricWindow<StepMetrics> prefillSteps,
                                MetricWindow<StepMetrics> decodeSteps) {
    }

    private static final class MetricWindow<T> {
        private final List<Metric<T>> metrics;
        private final FlexMetricTags tags;
        private final MetricAverage[] counters;
        private final double[] windowAverages;
        private ObjDoubleConsumer<String> report;

        MetricWindow(List<Metric<T>> metrics, FlexMetricTags tags) {
            this.metrics = metrics;
            this.tags = tags;
            this.counters = new MetricAverage[metrics.size()];
            this.windowAverages = new double[metrics.size()];
            for (int i = 0; i < counters.length; i++) {
                counters[i] = new MetricAverage();
            }
        }

        void add(T sample) {
            for (int i = 0; i < counters.length; i++) {
                double value = metrics.get(i).value().applyAsDouble(sample);
                if (Double.isFinite(value)) {
                    counters[i].add(value);
                }
            }
        }

        void report(FlexMonitor monitor) {
            for (int i = 0; i < counters.length; i++) {
                windowAverages[i] = counters[i].averageSinceLastReport();
            }
            for (int i = 0; i < counters.length; i++) {
                double average = windowAverages[i];
                if (Double.isNaN(average)) {
                    continue;
                }
                if (report == null) {
                    report = monitor.withTags(tags);
                }
                report.accept(metrics.get(i).name(), average);
            }
        }
    }

    private static final class MetricAverage {
        private final DoubleAdder sum = new DoubleAdder();
        private final LongAdder count = new LongAdder();
        // Only the reporting thread updates the previous totals.
        private double previousSum;
        private long previousCount;

        void add(double value) {
            sum.add(value);
            count.increment();
        }

        double averageSinceLastReport() {
            long currentCount = count.sum();
            long newSamples = currentCount - previousCount;
            if (newSamples == 0) {
                return Double.NaN;
            }
            // Writers may add a sample between these reads. Keep the counters cumulative
            // so sampling never pauses a writer or clears a concurrent update.
            double currentSum = sum.sum();
            double average = (currentSum - previousSum) / newSamples;
            previousSum = currentSum;
            previousCount = currentCount;
            return average;
        }
    }
}
