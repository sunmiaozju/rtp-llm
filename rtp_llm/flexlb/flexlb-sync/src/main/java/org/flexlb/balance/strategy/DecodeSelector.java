package org.flexlb.balance.strategy;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.DecodeEndpoint.CapacityRelease;
import org.flexlb.balance.scheduler.decode.DecodeMode;
import org.flexlb.balance.scheduler.decode.DecodeRequirement;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.RejectReason;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.sync.status.WorkerDirectory;
import org.flexlb.util.PriorityNormalizer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class DecodeSelector {

    private static final int MAX_SELECTION_ATTEMPTS = 2;
    private enum Availability { READY, BUSY, IMPOSSIBLE }
    private final WorkerDirectory workerDirectory;
    private final EndpointRoundRobin rotation;

    public DecodeSelector(WorkerDirectory workerDirectory,
                          EndpointRoundRobin rotation) {
        this.workerDirectory = workerDirectory;
        this.rotation = rotation;
    }

    public PlacementResult<DecodeEndpoint, RoleType> select(DecodeRequirement request, String group) {
        return select(null, request, group);
    }

    /**
     * Selects the least-cost available Decode endpoint and records the choice
     * reason when the caller supplies the request context.
     */
    public PlacementResult<DecodeEndpoint, RoleType> select(BalanceContext context,
                                                            DecodeRequirement request,
                                                            String group) {
        boolean immediate = request.mode() == DecodeMode.IMMEDIATE;
        for (int attempt = 0; attempt < MAX_SELECTION_ATTEMPTS; attempt++) {
            List<Map.Entry<String, DecodeEndpoint>> candidates = workerDirectory.getDecodeCandidates(group);
            if (candidates.isEmpty()) {
                return PlacementResult.blocked(RoleType.DECODE);
            }
            Availability[] availabilityByWorker = new Availability[candidates.size()];
            Availability preferredAvailability = Availability.IMPOSSIBLE;
            boolean allWorkersTooSmall = true;
            long largestKvBudget = 0L;
            for (int index = 0; index < candidates.size(); index++) {
                DecodeEndpoint endpoint = candidates.get(index).getValue();
                var engine = endpoint.getStatus().committedEngineObservation();
                long totalKv = Math.max(0L, engine.totalKvCacheTokens());
                long kvBudget = request.capacity().kvBudget(totalKv);
                Availability availability;
                if (totalKv > 0L && request.inputAndMaxOutputKvTokens() > kvBudget) {
                    availability = Availability.IMPOSSIBLE;
                } else {
                    availability = endpoint.hasCapacityForSelection(engine, request.capacity(), immediate,
                            request.inputKvTokens(), request.inputAndMaxOutputKvTokens())
                                    ? Availability.READY
                                    : Availability.BUSY;
                }
                availabilityByWorker[index] = availability;
                allWorkersTooSmall &= availability == Availability.IMPOSSIBLE;
                largestKvBudget = Math.max(largestKvBudget, kvBudget);
                if (availability == Availability.READY || availability == Availability.BUSY
                        && preferredAvailability == Availability.IMPOSSIBLE && !immediate) {
                    preferredAvailability = availability;
                }
            }
            if (allWorkersTooSmall) {
                return PlacementResult.rejected(oversizedRequestFailure(
                        request.inputAndMaxOutputKvTokens(), largestKvBudget));
            }
            if (preferredAvailability == Availability.IMPOSSIBLE) {
                return classifyCapacityFailure(request, candidates);
            }
            Availability selectedAvailability = preferredAvailability;
            double[] costByWorker = new double[candidates.size()];
            double minimumCost = Double.POSITIVE_INFINITY;
            int minimumCostCandidateCount = 0;
            for (int index = 0; index < candidates.size(); index++) {
                if (availabilityByWorker[index] == selectedAvailability) {
                    DecodeEndpoint endpoint = candidates.get(index).getValue();
                    var engine = endpoint.getStatus().committedEngineObservation();
                    double cost = request.costFormula()
                            .evaluate(endpoint.totalLoad(), request.capacity().maxEngineRequests(),
                                    endpoint.placementKvUsed(engine), Math.max(0L, engine.totalKvCacheTokens()));
                    costByWorker[index] = cost;
                    if (Double.isFinite(cost)) {
                        if (cost < minimumCost) {
                            minimumCost = cost;
                            minimumCostCandidateCount = 1;
                        } else if (cost == minimumCost) {
                            minimumCostCandidateCount++;
                        }
                    }
                }
            }
            if (!Double.isFinite(minimumCost)) {
                throw new IllegalStateException("Decode cost formula produced no finite score: "
                        + request.costFormula().expression());
            }
            double selectedCost = minimumCost;
            int selectedIndex = rotation.next(RoleType.DECODE, group, candidates.size(),
                    i -> availabilityByWorker[i] == selectedAvailability && costByWorker[i] == selectedCost,
                    i -> candidates.get(i).getKey());
            if (selectedIndex < 0) {
                throw new IllegalStateException("No Decode candidate matched the selected cost and availability");
            }
            DecodeEndpoint endpoint = workerDirectory.findDecodeEndpoint(candidates.get(selectedIndex));
            if (endpoint != null) {
                if (context != null) {
                    context.recordSelectionReason(RoleType.DECODE,
                            minimumCostCandidateCount > 1
                                    ? "DECODE_MIN_COST_ROUND_ROBIN_TIEBREAK"
                                    : "DECODE_MIN_COST");
                }
                return PlacementResult.success(endpoint);
            }
        }
        return PlacementResult.blocked(RoleType.DECODE);
    }

    private PlacementResult<DecodeEndpoint, RoleType> classifyCapacityFailure(
            DecodeRequirement request, List<Map.Entry<String, DecodeEndpoint>> candidates) {
        Response failure = null;
        boolean uniformFailure = true;
        List<Map<String, Object>> evidence = new ArrayList<>(candidates.size());
        for (Map.Entry<String, DecodeEndpoint> candidate : candidates) {
            Response workerFailure;
            DecodeEndpoint endpoint = workerDirectory.findDecodeEndpoint(candidate);
            if (endpoint == null) {
                workerFailure = Response.error(StrategyErrorType.RESOURCE_EXHAUSTED);
            } else {
                var snapshot = endpoint.admissionSummary();
                evidence.add(Map.of("endpoint", candidate.getKey(), "version", snapshot.routing().admissionVersion(),
                        "engineLoad", snapshot.routing().engineLoad(), "totalLoad", snapshot.routing().totalLoad(),
                        "kvTotal", snapshot.routing().totalKv(), "kvAvailable",
                        snapshot.routing().placementUsage().availableKvAfterReservations()));
                workerFailure = classifyCapacityFailure(request, snapshot);
            }
            if (failure == null) {
                failure = workerFailure;
            } else if (failure.getCode() != workerFailure.getCode()
                    || failure.getAdmissionRejectReason() != workerFailure.getAdmissionRejectReason()) {
                uniformFailure = false;
            }
            if (workerFailure.getCode() == StrategyErrorType.ADMISSION_UNAVAILABLE.getErrorCode()) {
                failure = workerFailure;
            }
        }
        if (failure == null || !uniformFailure && failure.getCode() != StrategyErrorType.ADMISSION_UNAVAILABLE.getErrorCode()) {
            failure = Response.error(StrategyErrorType.RESOURCE_EXHAUSTED);
        }
        return PlacementResult.blocked(RoleType.DECODE, failure, Map.of("cause", "Decode capacity exhausted",
                "capturedAtMs", System.currentTimeMillis(), "decode", List.copyOf(evidence)));
    }

    /** Classify only the dimensions that prevent this request from fitting. */
    static Response classifyCapacityFailure(DecodeRequirement request, DecodeEndpoint.AdmissionSummary snapshot) {
        boolean dispatch = request.mode() == DecodeMode.IMMEDIATE;
        var routing = snapshot.routing();
        var usage = dispatch ? routing.dispatchUsage() : routing.placementUsage();
        if (usage.totalKvTokens() > 0
                && request.inputAndMaxOutputKvTokens() > request.capacity().kvBudget(usage.totalKvTokens())) {
            return Response.error(StrategyErrorType.RESOURCE_EXHAUSTED);
        }
        CapacityRelease lower = CapacityRelease.NONE;
        CapacityRelease higher = CapacityRelease.NONE;
        CapacityRelease same = CapacityRelease.NONE;
        for (int priority = 1; priority <= PriorityNormalizer.MAX_PRIORITY; priority++) {
            CapacityRelease occupied = dispatch ? snapshot.engineOccupancy(priority) : snapshot.placementOccupancy(priority);
            if (occupied == CapacityRelease.NONE) { continue; }
            if (priority < request.priority()) {
                lower = lower.plus(occupied);
            } else if (priority == request.priority()) {
                same = occupied;
            } else {
                higher = higher.plus(occupied);
            }
        }
        // Removing lower-priority occupancy is a counterfactual for attribution,
        // not an authorization to preempt its owners.
        var residual = request.capacity().evaluate(usage, request.inputKvTokens(), request.inputAndMaxOutputKvTokens(), lower);
        if (residual.hasCapacity()) {
            return Response.error(StrategyErrorType.RESOURCE_EXHAUSTED);
        }
        CapacityRelease attributed = higher.plus(same);
        if (residual.requests() > attributed.requests()
                || residual.requiredKvTokens() > attributed.requiredKvTokens()
                || residual.kvBudgetTokens() > attributed.kvBudgetTokens()) {
            return Response.error(StrategyErrorType.ADMISSION_UNAVAILABLE);
        }
        boolean higherBlocks = residual.requests() > 0 && higher.requests() > 0
                || residual.requiredKvTokens() > 0 && higher.requiredKvTokens() > 0
                || residual.kvBudgetTokens() > 0 && higher.kvBudgetTokens() > 0;
        return Response.error(StrategyErrorType.PRIORITY_ADMISSION_REJECTED,
                higherBlocks ? RejectReason.HIGHER_PRIORITY_AHEAD : RejectReason.SAME_PRIORITY_AHEAD);
    }

    private static Response oversizedRequestFailure(long required, long maximum) {
        return Response.error(StrategyErrorType.RESOURCE_EXHAUSTED, RejectReason.RESOURCE_EXHAUSTED,
                "Decode prompt plus output demand=" + required + " exceeds every worker KV admission budget; maximum="
                        + maximum);
    }

}
