package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.DecodeTaskPhase;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Decode worker boundary: lifecycle holds, resource operations and lock-free notifications.
 * DecodeState owns the generation-local resource ledger and its single mutation lock.
 */
public class DecodeEndpoint extends WorkerEndpoint {
    private static final Logger logger = LoggerFactory.getLogger("syncLogger");
    private final DecodeState state;
    private final EndpointEventHandler endpointEvents;
    private final PlacementAvailability placementAvailability;
    private final Set<Runnable> engineDispatchCapacityListeners = ConcurrentHashMap.newKeySet();

    // Construction

    public DecodeEndpoint(WorkerStatus status, EndpointEventHandler endpointEvents) {
        this(status, endpointEvents, new PlacementAvailability());
    }

    DecodeEndpoint(WorkerStatus status,
                   EndpointEventHandler endpointEvents,
                   PlacementAvailability placementAvailability) {
        super(status);
        this.state = new DecodeState(status, endpointEvents.getConfigService(), endpointEvents.getRequestReporter());
        this.endpointEvents = endpointEvents;
        this.placementAvailability = placementAvailability;
    }

    // Reservation and release. Lifecycle holds remain valid for the entire handoff.

    public ReservationHandle reserve(EndpointHold hold,
                                     String requestId,
                                     long inputKvTokens,
                                     long inputAndMaxOutputKvTokens,
                                     int priority) {
        return reserve(hold, requestId, inputKvTokens, inputAndMaxOutputKvTokens, priority, null);
    }

    public ReservationHandle reserve(EndpointHold hold,
                                     String requestId,
                                     long inputKvTokens,
                                     long inputAndMaxOutputKvTokens,
                                     int priority,
                                     CapacityLimit capacity) {
        checkHold(hold);
        return state.reserve(requestId, inputKvTokens, inputAndMaxOutputKvTokens, priority, true, capacity);
    }

    /** An engine-facing shadow for work already outside the local queue. */
    public ReservationHandle reserveUnqueued(EndpointHold hold,
                                             String requestId,
                                             long inputKvTokens,
                                             long inputAndMaxOutputKvTokens,
                                             int priority) {
        checkHold(hold);
        ReservationHandle reservation = state.reserve(requestId, inputKvTokens, inputAndMaxOutputKvTokens, priority, false, null);
        if (reservation == null) {
            throw new IllegalStateException("Decode request id is already owned: " + requestId);
        }
        return reservation;
    }

    /** LOCAL_ROLLBACK requires local ownership; other evidence may leave Engine/protocol ownership intact. */
    public ReservationReleaseResult release(ReservationHandle reservation, ReleaseReason reason) {
        ReservationReleaseResult result = state.release(reservation, reason);
        if (result == ReservationReleaseResult.RELEASED) {
            publishCapacityRelease();
        }
        return result;
    }

    /**
     * 空机放行不检查 KV 额度，但空机检查与预占必须在同一节点锁内，避免多个消费者同时放行。
     */
    public ReservationHandle reserveIfEmpty(String requestId,
                                            long inputKvTokens,
                                            long inputAndMaxOutputKvTokens,
                                            int priority) {
        return state.reserveIfEmpty(requestId, inputKvTokens, inputAndMaxOutputKvTokens, priority);
    }

    /**
     * 发布成功后登记
     */
    public void registerPublished(ReservationHandle reservation, long publishedAtMs) {
        state.registerPublished(reservation, publishedAtMs);
    }

    public boolean isAcceptedByEngine(ReservationHandle reservation) { return state.isAcceptedByEngine(reservation); }

    public ReservationHandle reservationHandle(String requestId) {
        return isRetired() ? null : state.reservationHandle(requestId);
    }

    public record ReservationHandle(
            long endpointGenerationId,
            String requestId,
            long reservationToken) {

        public ReservationHandle {
            if (endpointGenerationId <= 0L || reservationToken <= 0L) {
                throw new IllegalArgumentException(
                        "Decode reservation identity must be positive");
            }
        }
    }

    public enum ReleaseReason {
        LOCAL_ROLLBACK, COUNTERPART_FINISHED, NOT_SENT, EXPIRED
    }

    public enum ReservationReleaseResult {
        RELEASED,
        ENGINE_ACCEPTED,
        STILL_OWNED,
        STALE,
        CONFLICT;

        public boolean released() { return this == RELEASED; }
    }

    // Dispatch: acquire capacity, hand over ownership, or return the permit.

    public boolean markQueued(EndpointHold hold, ReservationHandle reservation) {
        checkHold(hold);
        boolean changed = state.markQueued(reservation);
        if (changed) { publishCapacityRelease(); }
        return changed;
    }

    /**
     * V2 一次取得预占和下发名额；容量不足返回 null，成功后可直接发布。
     */
    public DispatchReservation reserveForDispatch(EndpointHold hold,
                                                  String requestId,
                                                  long inputKvTokens,
                                                  long inputAndMaxOutputKvTokens,
                                                  int priority,
                                                  CapacityLimit capacity) {
        checkHold(hold);
        DecodeState.ReservedDispatch reserved =
                state.reserveForDispatch(requestId, inputKvTokens, inputAndMaxOutputKvTokens, priority, capacity);
        return reserved == null
                ? null
                : new DispatchReservation(reserved.reservation(),
                        new EngineDispatchPermit(this, requestId, reserved.permit()));
    }

    public record DispatchReservation(ReservationHandle reservation, EngineDispatchPermit permit) {}

    public EngineDispatchPermitAcquisition acquireDispatchPermit(ReservationHandle reservation,
                                                                 CapacityLimit capacity) {
        EndpointHold hold = tryHold();
        if (hold == null) {
            return new EngineDispatchPermitAcquisition(EngineDispatchPermitAcquireStatus.ENDPOINT_RETIRED, null);
        }
        try (hold) {
            DecodeState.DispatchAcquisition acquired = state.acquireDispatchPermit(reservation, capacity);
            return new EngineDispatchPermitAcquisition(acquired.status(), acquired.permit() == null ? null
                    : new EngineDispatchPermit(this, reservation.requestId(), acquired.permit()));
        }
    }

    /** Resolve a permit once. ENGINE_OWNED means handoff, not Worker acceptance. */
    public EngineDispatchPermitTransferStatus dispatch(EngineDispatchPermit permit, DispatchOutcome outcome) {
        if (permit.endpoint != this) { throw new IllegalArgumentException("Dispatch permit belongs to another endpoint"); }
        return permit.resolve(outcome);
    }

    private EngineDispatchPermitTransferStatus applyDispatch(EngineDispatchPermit permit, DispatchOutcome outcome) {
        EndpointHold hold = outcome == DispatchOutcome.ENGINE_OWNED ? tryHold() : null;
        if (outcome == DispatchOutcome.ENGINE_OWNED && hold == null) {
            return EngineDispatchPermitTransferStatus.ENDPOINT_RETIRED;
        }
        try (hold) {
            DecodeState.DispatchResult result = state.dispatch(permit.lease, outcome);
            if (result.capacityReleased()) {
                if (outcome == DispatchOutcome.ABANDONED) { publishCapacityRelease(); } else { notifyEngineDispatchCapacityListeners(); }
            }
            return result.status();
        }
    }

    /** Lock-free waiter hint, including retirement or lost ownership. Acquisition still rechecks capacity. */
    public boolean shouldRetryDispatch(String requestId, CapacityLimit capacity) {
        return isRetired() || state.shouldRetryDispatch(requestId, capacity);
    }

    public static final class EngineDispatchPermit {

        private enum Resolution {
            SUCCESS,
            ENGINE_LIFECYCLE_OWNED,
            RELEASED,
            INVALIDATED,
            ENDPOINT_RETIRED
        }

        private final DecodeEndpoint endpoint;
        private final String requestId;
        private final DecodeState.DispatchLease lease;
        private Resolution resolution = Resolution.SUCCESS;

        private EngineDispatchPermit(DecodeEndpoint endpoint,
                                     String requestId,
                                     DecodeState.DispatchLease lease) {
            this.endpoint = endpoint;
            this.requestId = requestId;
            this.lease = lease;
        }

        public String requestId() {
            return requestId;
        }

        /**
         * Transfer this acquired slot without a second capacity check.
         *
         * @return a typed distinction between transfer, prior request-owner
         *         loss, and endpoint-generation retirement
         */
        public EngineDispatchPermitTransferStatus dispatch() {
            return endpoint.dispatch(this, DispatchOutcome.ENGINE_OWNED);
        }

        public boolean release() {
            return endpoint.dispatch(this, DispatchOutcome.ABANDONED)
                    == EngineDispatchPermitTransferStatus.TRANSFERRED;
        }

        private synchronized EngineDispatchPermitTransferStatus resolve(DispatchOutcome outcome) {
            if (outcome == DispatchOutcome.ENGINE_OWNED) {
                if (resolution == Resolution.ENGINE_LIFECYCLE_OWNED) {
                    return EngineDispatchPermitTransferStatus.TRANSFERRED;
                }
                if (resolution == Resolution.ENDPOINT_RETIRED) {
                    return EngineDispatchPermitTransferStatus.ENDPOINT_RETIRED;
                }
            }
            if (resolution != Resolution.SUCCESS) {
                return EngineDispatchPermitTransferStatus.OWNERSHIP_LOST;
            }
            EngineDispatchPermitTransferStatus result = endpoint.applyDispatch(this, outcome);
            resolution = switch (result) {
                case TRANSFERRED -> outcome == DispatchOutcome.ENGINE_OWNED
                        ? Resolution.ENGINE_LIFECYCLE_OWNED : Resolution.RELEASED;
                case ENDPOINT_RETIRED -> Resolution.ENDPOINT_RETIRED;
                case OWNERSHIP_LOST -> Resolution.INVALIDATED;
            };
            return result;
        }
    }

    public record EngineDispatchPermitAcquisition(
            EngineDispatchPermitAcquireStatus status,
            EngineDispatchPermit permit) {

        public EngineDispatchPermitAcquisition {
            if (status == null) {
                throw new IllegalArgumentException("permit acquisition status is required");
            }
            boolean ownsHandoff = status == EngineDispatchPermitAcquireStatus.SUCCESS
                    || status == EngineDispatchPermitAcquireStatus.ALREADY_ACCEPTED;
            if (ownsHandoff != (permit != null)) {
                throw new IllegalArgumentException(
                        "only SUCCESS or ALREADY_ACCEPTED results carry an engine dispatch permit");
            }
        }
    }

    public enum EngineDispatchPermitAcquireStatus {
        /** An exact-reservation permit now owns one Decode hard-gate slot. */
        SUCCESS,
        /** Engine already owns this reservation; the permit carries identity without charging capacity. */
        ALREADY_ACCEPTED,
        /** Concurrency or Decode KV has no unreserved hard capacity. */
        CAPACITY_FULL,
        /** The request no longer owns a live shadow reservation. */
        NOT_OWNED,
        /** The reservation is already engine-facing rather than Prefill-queued. */
        NOT_QUEUED,
        /** Another pre-delivery attempt already owns this request's permit. */
        ALREADY_ACQUIRED,
        /** This exact Decode endpoint generation no longer accepts delivery. */
        ENDPOINT_RETIRED
    }

    public enum EngineDispatchPermitTransferStatus {
        TRANSFERRED,
        OWNERSHIP_LOST,
        ENDPOINT_RETIRED
    }

    public enum DispatchOutcome { ENGINE_OWNED, ABANDONED }

    public void addEngineDispatchCapacityListener(Runnable listener) {
        if (listener != null) {
            engineDispatchCapacityListeners.add(listener);
        }
    }

    public void removeEngineDispatchCapacityListener(Runnable listener) {
        if (listener != null) {
            engineDispatchCapacityListeners.remove(listener);
        }
    }

    // Preemption: atomic local replacement or remote cancellation.

    public boolean replaceQueuedRequests(List<ReservationHandle> victims,
                                         String incomingRequestId,
                                         long inputKvTokens,
                                         long inputAndMaxOutputKvTokens,
                                         int priority,
                                         CapacityLimit capacity) {
        EndpointHold hold = tryHold();
        if (hold == null) {
            return false;
        }
        try (hold) {
            boolean replaced = state.replaceQueuedRequests(victims, incomingRequestId, inputKvTokens, inputAndMaxOutputKvTokens, priority, capacity);
            if (replaced) { publishCapacityRelease(); }
            return replaced;
        }
    }

    /** Atomically reserve an instruction-bearing route without issuing Cancel RPCs. */
    public PreemptionBeginResult beginReturnedPreemption(long attemptToken,
                                                         List<ReservationHandle> victims,
                                                         String incomingRequestId,
                                                         long inputKvTokens,
                                                         long inputAndMaxOutputKvTokens,
                                                         int priority,
                                                         CapacityLimit capacity) {
        if (attemptToken <= 0 || victims == null || victims.isEmpty()) {
            throw new IllegalArgumentException("attempt token and victims are required");
        }
        EndpointHold hold = tryHold();
        if (hold == null) {
            return PreemptionBeginResult.ENDPOINT_RETIRED;
        }
        try (hold) {
            return state.beginReturnedPreemption(
                    attemptToken, victims, incomingRequestId,
                    inputKvTokens, inputAndMaxOutputKvTokens, priority, capacity);
        }
    }

    public enum PreemptionBeginResult {
        SUCCESS,
        ENDPOINT_RETIRED,
        VICTIM_GONE,
        VICTIM_ALREADY_CLAIMED,
        INVALID_PRIORITY,
        INFEASIBLE,
        INCOMING_ALREADY_RESERVED,
        ATTEMPT_ALREADY_EXISTS
    }

    // Calibration: publish facts only after the resource transaction returns.

    public Runnable applyPreparedStatus(WorkerStatus ws, WorkerStatus.PreparedStatus prepared) {
        requireStatusGeneration(ws);
        if (!prepared.observation().alive()) { beginRetirement(); }
        DecodeState.CalibrationResult result;
        try {
            result = state.calibrate(prepared);
        } catch (RuntimeException | Error failure) {
            beginRetirement();
            throw failure;
        }
        notifyEngineDispatchCapacityListeners();
        if (result.capacityImproved()) {
            signalPlacementCapacityChanged();
        }
        return () -> { };
    }

    public Runnable initializeFromPreparedStatus(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        state.initialize(observation);
        return () -> { };
    }

    public Runnable observeStatusHeartbeat(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        state.observeHeartbeat(observation);
        return () -> { };
    }

    // Read-only resource and capacity views.

    public AdmissionSummary admissionSummary() {
        return state.admissionSummary();
    }

    public DecodeRoutingView routingView() { return state.routingView(); }

    DecodeRoutingView routingViewSnapshot(String address) { return state.routingViewSnapshot(address); }

    public LayeredAdmissionView resourceSnapshot() { return state.resourceSnapshot(); }

    public long placementVersion() { return state.placementVersion(); }

    public long realKvAvailable() { return state.realKvAvailable(); }

    public int getInflightCount() { return state.getInflightCount(); }

    public OptionalLong getLoadMetric() { return OptionalLong.of(state.getTotalLoad()); }

    public record LayeredAdmissionView(DecodeRoutingView routing,
                                       Map<String, DecodeRequestView> reserved,
                                       List<DecodeRequestView> confirmed,
                                       int queuedCount,
                                       int activeDispatchPermits) {

        public long admissionVersion() {
            return routing.admissionVersion();
        }

        public int acceptedCount() {
            return phaseCount(DecodeTaskPhase.ACCEPTED_NOT_RUNNING);
        }

        public int runningCount() {
            return phaseCount(DecodeTaskPhase.RUNNING);
        }

        public int engineCapacityUsed() {
            return routing.engineCapacityUsed();
        }

        public boolean isQueued(String requestId) {
            DecodeRequestView request = reserved.get(requestId);
            return request != null && request.queued();
        }

        private int phaseCount(DecodeTaskPhase phase) {
            int count = 0;
            for (DecodeRequestView task : confirmed) {
                if (task.phase() == phase) {
                    count++;
                }
            }
            return count;
        }
    }

    public record DecodeRoutingView(
            String address,
            long generationId,
            WorkerStatus.TopologySnapshot topology,
            WorkerStatus.CommittedWorkerStatus workerStatus,
            long admissionVersion,
            int totalLoad,
            int engineLoad,
            CapacityUsage placementUsage,
            CapacityUsage dispatchUsage,
            long inputKvReserved,
            long inputAndMaxOutputKvReserved) {

        public int engineCapacityUsed() { return Math.toIntExact(dispatchUsage.occupiedRequests()); }
        public long realKvUsed() { return placementUsage.kvBudgetUsedTokens(); }
        public long realKvAvailable() { return placementUsage.availableKvAfterReservations(); }
        public long engineFacingKvUsed() { return dispatchUsage.kvBudgetUsedTokens(); }
        public long engineFacingKvAvailable() { return dispatchUsage.availableKvAfterReservations(); }
        public long totalKv() { return placementUsage.totalKvTokens(); }

        public DecodeRoutingView {
            if (generationId <= 0L) {
                throw new IllegalArgumentException(
                        "Decode routing view requires a positive generation");
            }
        }
    }

    public record DecodeRequestView(String requestId,
                                    int priority,
                                    long kvTokens,
                                    long kvBudgetTokens,
                                    DecodeTaskPhase phase,
                                    boolean priorityKnown,
                                    long reservationToken,
                                    boolean queued,
                                    boolean claimedForPreemption) {

        public CapacityRelease placementRelease() {
            return new CapacityRelease(1L, kvTokens, kvBudgetTokens);
        }
    }

    /** Failure-only priority summary, shared until the admission revision changes. */
    public static final class AdmissionSummary {
        private final DecodeRoutingView routing;
        private final CapacityRelease[] placementOccupancy;
        private final CapacityRelease[] engineOccupancy;

        AdmissionSummary(DecodeRoutingView routing, CapacityRelease[] placementOccupancy,
                         CapacityRelease[] engineOccupancy) {
            this.routing = routing;
            this.placementOccupancy = placementOccupancy;
            this.engineOccupancy = engineOccupancy;
        }

        public DecodeRoutingView routing() { return routing; }
        public CapacityRelease placementOccupancy(int priority) { return placementOccupancy[priority]; }
        public CapacityRelease engineOccupancy(int priority) { return engineOccupancy[priority]; }
    }

    public record CapacityLimit(long maxEngineRequests, long maxKvUsagePercent) {

        public CapacityLimit {
            if (maxEngineRequests < 0L || maxKvUsagePercent <= 0L
                    || maxKvUsagePercent > RoutingConfig.PERCENTAGE_SCALE) {
                throw new IllegalArgumentException(
                        "Decode admission limits are outside their domain");
            }
        }

        public CapacityDeficit evaluate(CapacityUsage usage, long requiredKvTokens, long kvBudgetTokens) {
            return evaluate(usage, requiredKvTokens, kvBudgetTokens, CapacityRelease.NONE);
        }

        /** Use the same occupancy scope for the observation and every exact victim release. */
        public CapacityDeficit evaluate(CapacityUsage usage, long requiredKvTokens, long kvBudgetTokens,
                                        CapacityRelease release) {
            if (requiredKvTokens < 0L || kvBudgetTokens < requiredKvTokens) {
                throw new IllegalArgumentException("Decode demand must satisfy KV budget >= required KV >= 0");
            }
            long requests = maxEngineRequests == 0L ? 0L
                    : shortfall(Math.max(0L, usage.occupiedRequests - release.requests),
                            1L, maxEngineRequests, 0L);
            if (usage.totalKvTokens == 0L) {
                return new CapacityDeficit(requests, 0L, 0L);
            }
            return new CapacityDeficit(requests,
                    shortfall(usage.reservedKvTokens, requiredKvTokens,
                            usage.availableKvTokens, release.requiredKvTokens),
                    shortfall(Math.max(0L, usage.kvBudgetUsedTokens - release.kvBudgetTokens),
                            kvBudgetTokens, kvBudget(usage.totalKvTokens), 0L));
        }

        /** Integer arithmetic never rounds the configured KV budget up. */
        public long kvBudget(long totalKv) {
            if (totalKv < 0L) {
                throw new IllegalArgumentException("negative KV capacity");
            }
            return totalKv / 100L * maxKvUsagePercent + totalKv % 100L * maxKvUsagePercent / 100L;
        }

        private static long shortfall(long used, long incoming, long capacity, long released) {
            long remainingUsed = Math.max(0L, used - released);
            long remainingCapacity = DecodeState.saturatedAddNonNegative(capacity, Math.max(0L, released - used));
            return remainingUsed > remainingCapacity
                    ? DecodeState.saturatedAddNonNegative(remainingUsed - remainingCapacity, incoming)
                    : Math.max(0L, incoming - (remainingCapacity - remainingUsed));
        }
    }

    public record CapacityUsage(long occupiedRequests, long totalKvTokens, long availableKvTokens,
                                long reservedKvTokens, long kvBudgetUsedTokens) {
        public CapacityUsage {
            if (occupiedRequests < 0L || totalKvTokens < 0L || availableKvTokens < 0L
                    || reservedKvTokens < 0L || kvBudgetUsedTokens < 0L) {
                throw new IllegalArgumentException("Decode occupancy must be non-negative");
            }
        }

        public long availableKvAfterReservations() {
            return Math.max(0L, availableKvTokens - reservedKvTokens);
        }
    }

    /**
     * Capacity released by removing a request: before confirmation, input KV
     * and input plus max output; after confirmation, both use engine-reported KV.
     */
    public record CapacityRelease(long requests, long requiredKvTokens, long kvBudgetTokens) {
        public static final CapacityRelease NONE = new CapacityRelease(0L, 0L, 0L);

        public CapacityRelease {
            if (requests < 0L || requiredKvTokens < 0L || kvBudgetTokens < requiredKvTokens) {
                throw new IllegalArgumentException("invalid Decode capacity release");
            }
        }

        public CapacityRelease plus(CapacityRelease other) {
            return new CapacityRelease(DecodeState.saturatedAddNonNegative(requests, other.requests),
                    DecodeState.saturatedAddNonNegative(requiredKvTokens, other.requiredKvTokens),
                    DecodeState.saturatedAddNonNegative(kvBudgetTokens, other.kvBudgetTokens));
        }
    }

    /**
     * Remaining shortages against available KV and the configured KV budget.
     */
    public record CapacityDeficit(long requests, long requiredKvTokens, long kvBudgetTokens) {
        public boolean fits() { return requests == 0L && !needsKv(); }
        public boolean needsKv() { return requiredKvTokens > 0L || kvBudgetTokens > 0L; }
        public long kvTokens() { return Math.max(requiredKvTokens, kvBudgetTokens); }
    }

    // Retirement and finished request history.

    public boolean isRetired() { return isGenerationRetiringOrRetired(); }

    protected void closeEndpoint() {
        try {
            state.retire();
        } finally {
            notifyEngineDispatchCapacityListeners();
        }
    }

    void purgeFinishedRequests(long ttlMs) {
        state.purgeFinishedRequests(ttlMs);
    }

    // Metrics and shared lock-free capacity notifications.

    public void reportBatchMetrics(BatchSchedulerReporter reporter) {
        DecodeState.Stats stats = state.stats();
        String engineIp = getStatus().getMetricIpPort();
        reporter.reportInflightRequestCount(RoleType.DECODE.name(), engineIp, stats.inflight());
        reporter.reportDecodeTotalLoad(engineIp, stats.totalLoad());
        reporter.reportDecodeInputAndMaxOutputKvReserved(engineIp, stats.inputAndMaxOutputKvTokens());
        reporter.reportDecodeInputKvReserved(engineIp, stats.inputKvTokens());
        reporter.reportInflightMaxAgeMs(RoleType.DECODE.name(), engineIp, stats.oldestAgeMs());
    }

    public void reportAdmissionMetrics(RequestSchedulerReporter reporter) {
        DecodeState.AdmissionStats stats = state.admissionStats();
        String endpoint = getStatus().getMetricIpPort();
        reporter.reportDecodeReservedCount(endpoint, stats.reserved());
        reporter.reportDecodeShadowKvReserved(
                endpoint, stats.inputKvTokens());
        reporter.reportDecodeRunningCount(endpoint, stats.running());
        reporter.reportDecodeAcceptedCount(endpoint, stats.accepted());
        reporter.reportDecodeEngineLoad(endpoint, stats.engineLoad());
    }

    private void publishCapacityRelease() {
        notifyEngineDispatchCapacityListeners();
        signalPlacementCapacityChanged();
    }

    private void notifyEngineDispatchCapacityListeners() {
        for (Runnable listener : engineDispatchCapacityListeners) {
            try {
                listener.run();
            } catch (Throwable listenerFailure) {
                logger.warn("Decode capacity listener failed", listenerFailure);
            }
        }
    }

    private void signalPlacementCapacityChanged() {
        WorkerStatus.TopologySnapshot topology =
                getStatus().topologySnapshot();
        placementAvailability.capacityChanged(
                RoleType.DECODE, topology.group(), ipPort());
    }
}
