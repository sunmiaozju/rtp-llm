package org.flexlb.cache.match;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import org.flexlb.cache.domain.CacheHitComparisonResult;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.match.localstandby.LocalStandbyComparisonService;
import org.flexlb.config.ConfigService;
import org.flexlb.config.ObservabilityConfig;
import org.flexlb.dao.cache.HostCacheMatch;
import org.flexlb.dao.master.CacheHitFeedback;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.TaskPhase;
import org.flexlb.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Bounded request/role/generation correlation, independent of scheduling ownership.
 */
@Component
public final class CacheHitFeedbackTracker {
    private static final Logger LOG = LoggerFactory.getLogger(CacheHitFeedbackTracker.class);
    private static final Logger PV = LoggerFactory.getLogger("pvLogger");
    private final Cache<Key, Prediction> predictions;
    private final LocalStandbyComparisonService comparisonService;

    public CacheHitFeedbackTracker(LocalStandbyComparisonService comparisonService, ConfigService configService) {
        this.comparisonService = comparisonService;
        ObservabilityConfig.CacheHitConfig config = configService.loadBalanceConfig().getObservability().getCacheHit();
        this.predictions = Caffeine.newBuilder()
                .maximumSize(config.getFeedbackMaximumEntries())
                .expireAfterAccess(Duration.ofMillis(config.getFeedbackTimeoutMs()))
                .scheduler(Scheduler.systemScheduler())
                .build();
        configService.addUpdateListener(updated -> updateConfig(updated.getObservability().getCacheHit()));
    }

    private void updateConfig(ObservabilityConfig.CacheHitConfig config) {
        predictions.policy().eviction().orElseThrow().setMaximum(config.getFeedbackMaximumEntries());
        predictions.policy()
                .expireAfterAccess()
                .orElseThrow()
                .setExpiresAfter(config.getFeedbackTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    void track(String requestId,
               RoleType role,
               String group,
               WorkerStatus worker,
               long inputTokens,
               long predictedHitTokens,
               CacheMatchResult result,
               CompletableFuture<Long> theoryHitTokens) {
        HostCacheMatch match = result.hostMatch(worker);
        long blockSize = result.blockSize();
        boolean kvcm = result.source() == CacheMatchSource.KVCM && result.querySucceeded();
        CacheHitFeedback seed = new CacheHitFeedback("cache_hit_comparison", requestId, result.source().name(),
                role.name(), group, worker.getWorkerIdentity(), null, inputTokens, blockSize,
                result.querySucceeded() ? predictedHitTokens : -1,
                kvcm, match == null ? 0 : CacheMatchResult.matchedTokens(match.localMatchBlocks(), blockSize, inputTokens),
                match == null ? 0 : CacheMatchResult.matchedTokens(match.globalMatchBlocks(), blockSize, inputTokens), 0, 0);
        predictions.put(new Key(worker.getGenerationId(), role, requestId), new Prediction(seed,
                comparisonService.captureComparison(requestId, role), theoryHitTokens));
    }

    List<CompletableFuture<CacheHitComparisonResult>> observe(WorkerStatus worker,
                                                             WorkerStatus.StatusObservation status) {
        List<CompletableFuture<CacheHitComparisonResult>> results = new ArrayList<>();
        // Prefer terminal telemetry if the Engine includes the task in both lists.
        status.finishedTasks().values().forEach(task -> observeTaskBestEffort(worker, status.role(), task, true, results));
        status.activeTasks().values().forEach(task -> observeTaskBestEffort(worker, status.role(), task, false, results));
        return results;
    }

    private void observeTaskBestEffort(WorkerStatus worker,
                                       RoleType role,
                                       WorkerStatus.TaskObservation task,
                                       boolean finished,
                                       List<CompletableFuture<CacheHitComparisonResult>> results) {
        try {
            observeTask(worker, role, task, finished, results);
        } catch (RuntimeException error) {
            LOG.warn("Failed to record cache-hit feedback for task: requestId={}, role={}",
                    task == null ? null : task.requestId(), role, error);
        }
    }

    private void observeTask(WorkerStatus worker,
                             RoleType role,
                             WorkerStatus.TaskObservation task,
                             boolean finished,
                             List<CompletableFuture<CacheHitComparisonResult>> results) {
        Key key = new Key(worker.getGenerationId(), role, task.requestId());
        Prediction prediction = predictions.getIfPresent(key);
        if (prediction == null) {
            return;
        }
        try {
            reportTaskFeedback(prediction, role, task, finished, results);
        } finally {
            if (finished) {
                predictions.asMap().remove(key, prediction);
            }
        }
    }

    private void reportTaskFeedback(Prediction prediction,
                                    RoleType role,
                                    WorkerStatus.TaskObservation task,
                                    boolean finished,
                                    List<CompletableFuture<CacheHitComparisonResult>> results) {
        CacheHitFeedback seed = prediction.seed;
        String state = finished ? "FINISHED" : task.phase() == null ? "UNKNOWN" : task.phase().name();
        boolean actualAvailable = task.prefixLengthValid()
                && (finished || task.phase() == TaskPhase.RUNNING)
                && task.prefixLength() >= 0 && task.prefixLength() <= seed.inputTokens();
        if (finished || actualAvailable) {
            Function<CacheHitFeedback, CompletableFuture<CacheHitComparisonResult>> comparison =
                    prediction.comparison.getAndSet(null);
            if (comparison != null) {
                CacheHitFeedback feedback = new CacheHitFeedback(seed.eventType(), seed.requestId(),
                        seed.cacheMatchSource(), seed.role(), seed.group(), seed.workerIdentity(), state,
                        seed.inputTokens(), seed.blockSize(), seed.predictedHitTokens(), seed.kvcmMatchAvailable(),
                        seed.kvcmLocalMatchTokens(), seed.kvcmGlobalMatchTokens(),
                        actualAvailable ? task.prefixLength() : -1,
                        actualAvailable && seed.predictedHitTokens() >= 0
                                ? task.prefixLength() - seed.predictedHitTokens() : -1);
                results.add(comparison.apply(feedback).thenCombine(
                        prediction.theoryHitTokens.copy().completeOnTimeout(null, 1, TimeUnit.SECONDS),
                        CacheHitComparisonResult::withTheoryHitTokens));
            }
        }
        if ((finished || task.firstTokenTimeMs() > 0) && prediction.statusLogged.compareAndSet(false, true)) {
            Map<String, Object> pv = new LinkedHashMap<>(32);
            pv.put("event", "prefill_worker_status");
            pv.put("requestId", task.requestId());
            pv.put("role", role.name());
            pv.put("group", seed.group());
            pv.put("workerIp", seed.workerIdentity().getIp());
            pv.put("worker", seed.logicalWorkerId());
            pv.put("engineIndex", seed.engineIndex());
            pv.put("state", state);
            pv.put("inputTokens", seed.inputTokens());
            pv.put("engineInputTokens", positive(task.inputLength()));
            pv.put("inputTokensDelta", task.inputLength() > 0 ? task.inputLength() - seed.inputTokens() : null);
            pv.put("batchId", task.batchId());
            pv.put("errorCode", task.errorCode());
            pv.put("prefixLengthValid", task.prefixLengthValid());
            pv.put("actualHitTokens", task.prefixLengthValid() ? task.prefixLength() : null);
            pv.put("requestReceivedTimeMs", positive(task.requestReceivedTimeMs()));
            pv.put("inputQueueEnqueueTimeMs", positive(task.inputQueueEnqueueTimeMs()));
            pv.put("inputQueueDrainTimeMs", positive(task.inputQueueDrainTimeMs()));
            pv.put("firstTokenTimeMs", positive(task.firstTokenTimeMs()));
            pv.put("inputQueueWaitMs", elapsed(task.inputQueueDrainTimeMs(), task.inputQueueEnqueueTimeMs()));
            Long schedulerToRunning = elapsed(task.runningEnteredTimeMs(), task.waitingEnteredTimeMs());
            pv.put("schedulerToRunningMs", schedulerToRunning);
            pv.put("remoteKvWaitMs", schedulerToRunning == null ? null : task.remoteKvWaitMs());
            pv.put("schedulerWaitMs", schedulerToRunning == null || schedulerToRunning < task.remoteKvWaitMs()
                    ? null : schedulerToRunning - task.remoteKvWaitMs());
            pv.put("runningToFirstTokenMs", elapsed(task.firstTokenTimeMs(), task.runningEnteredTimeMs()));
            if (task.prefixLengthValid()
                    && task.hbmLocalMatchTokens() + task.remoteKvAddedMatchTokens() == task.prefixLength()) {
                pv.put("hbmLocalMatchTokens", task.hbmLocalMatchTokens());
                pv.put("remoteKvAddedMatchTokens", task.remoteKvAddedMatchTokens());
            }
            pv.put("firstPrefillStepId", positive(task.firstPrefillStepId()));
            pv.put("lastPrefillStepId", positive(task.lastPrefillStepId()));
            pv.put("prefillStepCount", positive(task.prefillStepCount()));
            pv.put("prefillNonfinalChunkTokensMin", positive(task.prefillNonfinalChunkTokensMin()));
            pv.put("prefillNonfinalChunkTokensMax", positive(task.prefillNonfinalChunkTokensMax()));
            PV.info(JsonUtils.toStringOrEmpty(pv));

        }
    }

    private static Long positive(long value) {
        return value > 0 ? value : null;
    }

    private static Long elapsed(long end, long start) {
        return start > 0 && end >= start ? end - start : null;
    }

    private record Key(long generation, RoleType role, String requestId) {
    }

    private static final class Prediction {
        private final CacheHitFeedback seed;
        private final CompletableFuture<Long> theoryHitTokens;
        private final AtomicReference<Function<CacheHitFeedback, CompletableFuture<CacheHitComparisonResult>>> comparison;
        private final AtomicBoolean statusLogged = new AtomicBoolean();

        private Prediction(CacheHitFeedback seed,
                           Function<CacheHitFeedback, CompletableFuture<CacheHitComparisonResult>> comparison,
                           CompletableFuture<Long> theoryHitTokens) {
            this.seed = seed;
            this.comparison = new AtomicReference<>(comparison);
            this.theoryHitTokens = theoryHitTokens;
        }
    }
}
