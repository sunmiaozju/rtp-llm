package org.flexlb.cache.match;

import lombok.extern.slf4j.Slf4j;
import org.flexlb.cache.domain.CacheHitComparisonResult;
import org.flexlb.cache.domain.CacheMatchQuery;
import org.flexlb.cache.domain.CacheMatchResult;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.domain.WorkerCacheUpdateResult;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Unified cache matching and metadata update service. */
@Slf4j
@Service
public class CacheAwareService {

    private final CacheMatchQueryOrchestrator queryOrchestrator;
    private final CacheHitFeedbackTracker feedbackTracker;
    private final CacheMetadataUpdateOrchestrator updateOrchestrator;

    public CacheAwareService(CacheMatchQueryOrchestrator queryOrchestrator,
                             CacheHitFeedbackTracker feedbackTracker,
                             CacheMetadataUpdateOrchestrator updateOrchestrator) {
        this.queryOrchestrator = queryOrchestrator;
        this.feedbackTracker = feedbackTracker;
        this.updateOrchestrator = updateOrchestrator;
    }

    public CacheMatchResult findMatchingEngines(CacheMatchQuery query) {
        long startTimeUs = System.nanoTime() / 1_000;
        try {
            return queryOrchestrator.findMatchingEngines(query);
        } catch (RuntimeException error) {
            CacheMatchSource source = queryOrchestrator.effectiveSource();
            long queryTimeUs = System.nanoTime() / 1_000 - startTimeUs;
            log.error("Error finding matching engines, requestId={}, role={}",
                    query.requestId(), query.roleType(), error);
            return CacheMatchResult.failed(source, queryTimeUs);
        }
    }

    public WorkerCacheUpdateResult updateFromWorkerStatus(WorkerStatus workerStatus) {
        return updateOrchestrator.updateFromWorkerStatus(workerStatus);
    }

    public void removeEngineBlockCache(String engineIpPort) {
        updateOrchestrator.removeEngineBlockCache(engineIpPort);
    }

    public void updateFromRoutedRequest(
            Request request, List<ServerStatus> selectedWorkers) {
        updateOrchestrator.updateFromRoutedRequest(request, selectedWorkers);
    }

    public void trackRoutingPrediction(String requestId,
                                       RoleType role,
                                       String group,
                                       WorkerStatus worker,
                                       long inputTokens,
                                       long predictedHitTokens,
                                       CacheMatchResult result,
                                       CompletableFuture<Long> theoryHitTokens) {
        try {
            feedbackTracker.track(requestId, role, group, worker, inputTokens, predictedHitTokens, result, theoryHitTokens);
        } catch (RuntimeException error) {
            log.warn("Cache prediction telemetry failed, requestId={}", requestId, error);
        }
    }

    public List<CompletableFuture<CacheHitComparisonResult>> observeCacheHitFeedback(WorkerStatus worker,
                                                                                     WorkerStatus.StatusObservation status) {
        return feedbackTracker.observe(worker, status);
    }
}
