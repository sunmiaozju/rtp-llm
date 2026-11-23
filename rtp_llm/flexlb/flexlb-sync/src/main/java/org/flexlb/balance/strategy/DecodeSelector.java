package org.flexlb.balance.strategy;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.scheduler.decode.DecodeRequirement;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.RejectReason;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.sync.status.WorkerDirectory;
import org.springframework.stereotype.Component;

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
                    availability = endpoint.hasCapacityForSelection(engine, request.capacity(), request.inputKvTokens(),
                            request.inputAndMaxOutputKvTokens()) ? Availability.READY : Availability.BUSY;
                }
                availabilityByWorker[index] = availability;
                allWorkersTooSmall &= availability == Availability.IMPOSSIBLE;
                largestKvBudget = Math.max(largestKvBudget, kvBudget);
                if (availability == Availability.READY
                        || availability == Availability.BUSY && preferredAvailability == Availability.IMPOSSIBLE) {
                    preferredAvailability = availability;
                }
            }
            if (allWorkersTooSmall) {
                return PlacementResult.rejected(oversizedRequestFailure(
                        request.inputAndMaxOutputKvTokens(), largestKvBudget));
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

    private static Response oversizedRequestFailure(long required, long maximum) {
        return Response.error(StrategyErrorType.RESOURCE_EXHAUSTED, RejectReason.RESOURCE_EXHAUSTED,
                "Decode prompt plus output demand=" + required + " exceeds every worker KV admission budget; maximum="
                        + maximum);
    }

}
