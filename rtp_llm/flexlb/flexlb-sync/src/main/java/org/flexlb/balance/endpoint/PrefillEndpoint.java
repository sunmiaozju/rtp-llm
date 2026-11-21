package org.flexlb.balance.endpoint;

import lombok.Getter;
import org.flexlb.balance.delivery.DeliveryBlockReason;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.prefill.CommitResult;
import org.flexlb.balance.endpoint.prefill.HeartbeatResult;
import org.flexlb.balance.endpoint.prefill.PrefillState;
import org.flexlb.balance.endpoint.prefill.PrefillStats;
import org.flexlb.balance.endpoint.prefill.ReservationResult;
import org.flexlb.balance.endpoint.prefill.ShutdownResult;
import org.flexlb.balance.endpoint.prefill.WorkerStatusFact;
import org.flexlb.balance.endpoint.prefill.WorkerStatusResult;
import org.flexlb.balance.endpoint.prefill.single.SingleReservation;
import org.flexlb.balance.estimate.PrefillTtftEstimate;
import org.flexlb.balance.estimate.PrefillTtftEstimator;
import org.flexlb.balance.prediction.FormulaPredictor;
import org.flexlb.balance.prediction.LearningPredictor;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.queue.QueuedRequestsSnapshot;
import org.flexlb.balance.queue.WorkerBatcher;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.loadbalance.RejectReason;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.PrefillQueueEnqueueResult;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PrefillEndpoint extends WorkerEndpoint {

    /**
     * Short-lived generation capability for committing one NON_BATCH route
     * group. Queued requests keep their canonical identity without keeping an
     * endpoint generation alive while waiting for delivery.
     */
    public final class RouteCommitAdmission implements AutoCloseable {
        private EndpointGenerationLifecycle.HandoffPermit workerKeepAlive;

        private RouteCommitAdmission(EndpointGenerationLifecycle.HandoffPermit workerKeepAlive) {
            this.workerKeepAlive = workerKeepAlive;
        }

        public CommitResult commit(List<ScheduledRequest> exactItems, List<SingleReservation> exactReservations) {
            EndpointGenerationLifecycle.HandoffPermit exact = workerKeepAlive;
            if (exact == null) {
                throw new IllegalStateException(
                        "route commit no longer owns its generation handoff");
            }
            CommitResult committed = prefillState.commitSingleGroup(exactItems, exactReservations, exact);
            workerKeepAlive = null;
            return committed;
        }

        @Override
        public void close() {
            EndpointGenerationLifecycle.HandoffPermit exact = workerKeepAlive;
            workerKeepAlive = null;
            if (exact != null) {
                exact.close();
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger("syncLogger");
    @Getter
    private final PrefillTimePredictor predictor;
    private final Supplier<FlexlbConfig> configSupplier;
    private final WorkerBatcher workerBatcher;
    private final PrefillQueue prefillQueue;
    private final PrefillQueueRegistry prefillQueueRegistry;
    private final PrefillState prefillState;
    private final EndpointEventHandler endpointEvents;
    private final BatchSchedulerReporter reporter;
    private final PlacementAvailability placementAvailability;
    private final EndpointRegistry endpointRegistry;
    private final QueueCapacity queueCapacity;

    PrefillEndpoint(WorkerStatus status,
                    Supplier<FlexlbConfig> configSupplier,
                    DeliveryStrategy deliveryStrategy,
                    PrefillTtftEstimator ttftEstimator,
                    EndpointEventHandler endpointEvents,
                    BatchSchedulerReporter reporter,
                    PlacementAvailability placementAvailability,
                    EndpointRegistry endpointRegistry,
                    QueueCapacity queueCapacity,
                    PrefillQueueRegistry prefillQueueRegistry,
                    RequestSchedulerReporter requestReporter) {
        super(status);
        this.configSupplier = configSupplier;
        this.reporter = reporter;
        this.endpointEvents = endpointEvents;
        this.placementAvailability = placementAvailability;
        this.endpointRegistry = endpointRegistry;
        this.queueCapacity = queueCapacity;
        this.prefillQueueRegistry = prefillQueueRegistry;
        FlexlbConfig startupConfig = configSupplier.get();
        this.predictor = createPredictor(startupConfig);
        this.workerBatcher = new WorkerBatcher(
                status.getLogicalIpPort(), this, configSupplier.get(),
                deliveryStrategy, ttftEstimator, endpointEvents, queueCapacity);
        this.prefillState = workerBatcher.ownedState();
        this.prefillQueue = new PrefillQueue(this, startupConfig.isPriorityOrdering(), requestReporter);
    }

    /**
     * 由这个节点创建并持有的 Prefill 队列。
     */
    public PrefillQueue prefillQueue() {
        return prefillQueue;
    }

    /** Start the attached generation exactly once before routing publication. */
    void startGeneration() {
        workerBatcher.start();
    }

    @Override
    void onPublished() {
        prefillQueue.initializeFromCachedStatus();
        prefillQueueRegistry.start(this);
    }

    /** Capture the canonical queue/work inputs for one pure route estimate. */
    public PrefillTtftEstimate.LoadView captureLoadView() {
        return workerBatcher.captureLoadView();
    }

    /** Stable delivery semantics selected once for this endpoint generation. */
    public PrefillTtftEstimate.TtftEstimationRule ttftEstimationRule() {
        return workerBatcher.ttftEstimationRule();
    }

    /**
     * Publish one exact route after validating its endpoint hold.
     */
    public PrefillQueueEnqueueResult offerHeldResult(EndpointHold exactHold, ScheduledRequest exactItem) {
        checkHold(exactHold);
        return workerBatcher.offerWithResult(exactItem);
    }

    /** Publish a role/group-scoped edge after real queue or status progress. */
    public void signalPlacementCapacityChanged() {
        WorkerStatus.TopologySnapshot topology =
                getStatus().topologySnapshot();
        placementAvailability.capacityChanged(
                getStatus().getRole(), topology.group(), ipPort());
    }

    /** Remove only the supplied canonical ACTIVE queue identity. */
    public boolean removeQueued(
            ScheduledRequest exactItem,
            String reason) {
        return workerBatcher.removeQueued(exactItem, reason);
    }

    /** Read the last scheduling decision without traversing or locking the queue. */
    public Map<String, Object> queueWaitDiagnostics() {
        return workerBatcher.waitDiagnostics();
    }

    /** Capture exact queued identities for eviction planning. */
    public QueuedRequestsSnapshot captureQueueSnapshot() {
        return workerBatcher.captureQueueSnapshot();
    }

    public int queuedRequestCount() {
        return workerBatcher.queueSize();
    }

    /**
     * Current queue sizes used for role-wide priority metrics.
     */
    public Map<Integer, Integer> queuedRequestCountsByPriority() {
        return workerBatcher.queueSizeByPriority();
    }

    /**
     * Runs once, after every accepted generation handoff has released its hold.
     */
    @Override
    protected void closeEndpoint() {
        prefillQueue.removeDispatchedOnRetirement();
        prefillQueueRegistry.retire(this);
        // A self-await invariant escapes here before ledger mutation/event
        // publication. Ordinary stop cleanup failures are returned only after
        // the exact worker has exited, and are aggregated below.
        Throwable retirementFailure = workerBatcher.stopAndAwait();
        try {
            ShutdownResult shutdownResult = prefillState.shutdown();
            if (!shutdownResult.removedItems().isEmpty()) {
                try {
                    endpointEvents.onPrefillGenerationRetired(this, shutdownResult.removedItems());
                } catch (Throwable callbackFailure) {
                    retirementFailure = appendRetirementFailure(
                            retirementFailure, callbackFailure);
                }
            }
            retirementFailure = appendRetirementFailure(retirementFailure, shutdownResult.checkFailure());
        } catch (Throwable committedRetirementFailure) {
            retirementFailure = appendRetirementFailure(
                    retirementFailure, committedRetirementFailure);
        }
        rethrowEndpointRetirementFailure(retirementFailure);
    }

    private static Throwable appendRetirementFailure(
            Throwable first,
            Throwable next) {
        if (next == null) {
            return first;
        }
        if (first == null) {
            return next;
        }
        // Cleanup is total even under allocation failure. Preserve the first
        // causal failure; later leaf failures must not escape aggregation.
        return first;
    }

    private static void rethrowEndpointRetirementFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "Prefill endpoint retirement failed", failure);
        }
    }

    private static PrefillTimePredictor createPredictor(FlexlbConfig config) {
        RoutingConfig.ExecutionTimeEstimatorConfig estimator = config.getRouter()
                .getRoles().getPrefill().getExecutionTimeEstimator();
        if (estimator.getType() == RoutingConfig.EstimatorType.LEARNING) {
            return new LearningPredictor();
        }
        return new FormulaPredictor(estimator.getExpression());
    }

    /**
     * Bind exact NON_BATCH ownership inside the caller's held placement
     * transaction.
     */
    public ReservationResult<SingleReservation> reserveSingle(ScheduledRequest exactItem,
                                                                                  long predictedMs) {
        if (isGenerationRetiringOrRetired()) {
            return ReservationResult.fail(ReservationResult.Status.ENDPOINT_RETIRED);
        }
        return prefillState.reserveSingle(
                exactItem, predictedMs);
    }

    public boolean isRetired() {
        return isGenerationRetiringOrRetired();
    }

    /** Acquire the generation capability only for the final route commit. */
    public RouteCommitAdmission tryBeginRouteCommitAdmission() {
        EndpointGenerationLifecycle.HandoffPermit handoffPermit =
                tryAcquireGenerationHandoff();
        return handoffPermit == null
                ? null : new RouteCommitAdmission(handoffPermit);
    }

    /** Explain a failed admission using the current owner-priority summary. */
    public RejectReason rejectReason(int priority, long inflightRequestLimit) {
        return prefillState.rejectReason(priority, inflightRequestLimit);
    }

    /** Advisory capacity; publication repeats the count check under the ownership lock. */
    public boolean canAcceptRequest(long inflightRequestLimit) {
        return inflightRequestLimit == 0L || prefillState.canAcceptRequest(inflightRequestLimit);
    }

    public boolean tryReserveEngineSubQueueSlot(FlexlbConfig config) {
        RoleType role = getStatus().getRole();
        int logicalEndpointCount = endpointRegistry.getEndpointCount(role);
        return queueCapacity.tryReserveEngineSubQueueSlot(config, role, logicalEndpointCount);
    }

    public boolean canDeliver(long maxDispatchLimit, int newReqNum) {
        return prefillState.canDeliver(maxDispatchLimit, newReqNum);
    }

    public DeliveryBlockReason.Availability routeAdmissionAvailability(long limit) {
        return prefillState.routeAvailability(limit);
    }

    public boolean canPreemptQueuedRequest(int priority, long inflightRequestLimit) {
        return prefillState.canPreemptQueuedRequest(priority, inflightRequestLimit);
    }

    /** Advisory endpoint ownership revision captured by queue placement. */
    public long placementVersion() {
        return prefillState.mutationVersion();
    }

    /** Exact counterpart cleanup; stale item generations are a no-op. */
    public boolean releaseCommittedItem(ScheduledRequest exactItem) {
        return prefillState.removeCommittedRequest(exactItem);
    }

    /**
     * End the exact failed request, whether still queued or committed.
     */
    public void settleFailedRequest(ScheduledRequest exactItem) {
        removeQueued(exactItem, "REQUEST_FAILED");
        releaseCommittedItem(exactItem);
    }

    /** Expire an exact local committed lease without claiming Engine completion. */
    public boolean expireCommittedItem(ScheduledRequest exactItem) {
        return prefillState.removeCommittedRequest(exactItem);
    }

    /**
     * 已提交、还没结束的请求数，包括批次成员和单个请求。
     */
    public int getCommittedRequestCount() {
        PrefillStats stats = prefillState.stats();
        return stats.committedRequests();
    }

    /**
     * 已提交、还没结束的单个请求数（NON_BATCH）。
     */
    public int getSingleRequestCount() {
        PrefillStats stats = prefillState.stats();
        return stats.singleRequests();
    }

    @Override
    public Runnable applyPreparedStatus(WorkerStatus ws, WorkerStatus.PreparedStatus prepared) {
        requireStatusGeneration(ws);
        WorkerStatus.StatusObservation observation = prepared.observation();
        WorkerStatusResult result =
                prefillState.updateFromWorkerStatus(observation, () -> {
                            if (!observation.alive()) {
                                beginRetirement();
                            }
                            workerBatcher.signalSchedulingInputsChanged();
                            ws.publishPreparedStatus(prepared);
                        },
                        this::beginRetirement);
        rethrowPublicationFailure(result.publicationFailure());
        List<WorkerStatusFact> facts = result.schedulerFacts();
        return () -> endpointEvents.onPrefillStatus(this, observation, facts);
    }

    @Override
    public Runnable initializeFromPreparedStatus(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        WorkerStatusResult result = prefillState.updateFromWorkerStatus(observation,
                workerBatcher::signalSchedulingInputsChanged, this::beginRetirement);
        if (!result.schedulerFacts().isEmpty()) {
            throw new IllegalStateException(
                    "Private Prefill candidate produced locally-owned status facts");
        }
        rethrowPublicationFailure(result.publicationFailure());
        return () -> { };
    }

    @Override
    public Runnable observeStatusHeartbeat(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        if (observation.owner() != ws) {
            throw new IllegalArgumentException(
                    "Status observation belongs to another Prefill generation");
        }
        HeartbeatResult result = prefillState.updateFromHeartbeat(observation);
        if (result.schedulingInputsChanged()) {
            workerBatcher.signalSchedulingInputsChanged();
        }
        return () -> endpointEvents.onPrefillStatus(this, observation, result.schedulerFacts());
    }

    private static void rethrowPublicationFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "Prefill status publication failed", failure);
        }
    }

    // ==================== Ownership diagnostics ====================

    /** Diagnostic snapshot of canonical local plus worker-reported ownership. */
    public long observedRequestCount() {
        return prefillState.observedRequestCount();
    }

    /** Evict stale individual requests whose IDs are absent from the scheduler directory. */
    public int evictExpiredRequests(long ttlMs,
                                    Predicate<String> retainForSchedulerCleanup) {
        return prefillState.evictExpiredSingles(
                ttlMs, retainForSchedulerCleanup);
    }

    /** Evict endpoint orphans while retaining IDs still registered by the scheduler. */
    public int evictExpiredInflight(long ttlMs,
                                    Predicate<String> retainForSchedulerCleanup) {
        return evictExpiredRequests(ttlMs, retainForSchedulerCleanup);
    }

    @Override
    public OptionalLong getLoadMetric() {
        return prefillState.committedSnapshot()
                .totalRemainingWorkMs();
    }

    // ==================== Metrics ====================

    /**
     * Report per-worker batch metrics via the given reporter.
     * Called periodically by {@link org.flexlb.balance.scheduler.RequestScheduler}.
     */
    public void reportBatchMetrics(BatchSchedulerReporter reporter) {
        String engineIp = getStatus().getMetricIpPort();
        String role = getStatus().getRole().name();
        int queueSize = workerBatcher.queueSize();
        reporter.reportBatcherQueueSize(role, engineIp, queueSize);
        PrefillStats stats = prefillState.stats();
        if (configSupplier.get().getDispatcher().getType() == DispatcherConfig.Type.NON_BATCH) {
            reporter.reportInflightRequestCount(role, engineIp, prefillState.dispatchedRequestCount());
            reporter.reportWorkerStatusUnconfirmedRequestCount(role, engineIp, stats.waitingForWorkerStatusRequests());
        } else {
            reporter.reportInflightRequestCount(role, engineIp, stats.waitingForWorkerStatusRequests());
        }
        reporter.reportInflightMaxAgeMs(role, engineIp, stats.maxStatusAgeMs());
        reportQueueMetricSafely("oldest endpoint queue wait", () ->
                reporter.reportEngineSubQueueOldestWaitMs(role, engineIp, workerBatcher.oldestSubQueueWaitMs()));
    }

    public void reportSubQueueResidence(ScheduledRequest item) {
        long durationMs = item.finishSubQueueVisitMs();
        if (durationMs < 0L) {
            return;
        }
        item.ctx().setSubQueueMs(item.ctx().getSubQueueMs() + durationMs);
        reportQueueMetricSafely("endpoint queue residence time", () ->
            reporter.reportEngineSubQueueResidenceMs(
                    getStatus().getRole().name(), getStatus().getMetricIpPort(), durationMs));
    }

    public void reportSharedSubQueueBlocked() {
        reportQueueMetricSafely("shared endpoint queue admission block", () ->
                reporter.reportSharedSubQueueBlocked(
                        getStatus().getRole().name(), getStatus().getMetricIpPort()));
    }

    private void reportQueueMetricSafely(String metricName, Runnable report) {
        try {
            report.run();
        } catch (Throwable failure) {
            try {
                logger.warn("Failed to report {}", metricName, failure);
            } catch (Throwable ignoredLoggingFailure) {
                // Metrics must not affect queue admission or delivery.
            }
        }
    }

}
