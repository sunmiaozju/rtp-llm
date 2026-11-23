package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.DecodeRequirement;
import org.flexlb.dao.route.RoleType;
import org.flexlb.sync.status.DpGroupMembers;
import org.flexlb.sync.status.WorkerDirectory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class DecodeSelector {

    private static final int DEFAULT_WORKER_LIST_CAPACITY = 16;

    private final WorkerDirectory workerDirectory;
    private long lastSeenDpSize;
    private int lastEmptyWorkerPeakSize = DEFAULT_WORKER_LIST_CAPACITY;
    private int lastLowestCostWorkerPeakSize = DEFAULT_WORKER_LIST_CAPACITY;

    public DecodeSelector(WorkerDirectory workerDirectory) {
        this.workerDirectory = workerDirectory;
    }

    public SelectedDecode select(BalanceContext context, String group) {
        var directory = workerDirectory.decodeRoutingEntries();
        int workerCount = directory.size();
        if (workerCount == 0) {
            return null;
        }

        DecodeRequirement requirement = context.getDecodeRequirement();
        Map<String, DpGroupMembers> dpGroups = null;
        var emptyWorkers = new ArrayList<Map.Entry<String, DecodeEndpoint>>(lastEmptyWorkerPeakSize);
        var lowestCostWorkers = new ArrayList<Map.Entry<String, DecodeEndpoint>>(lastLowestCostWorkerPeakSize);
        double lowestCost = Double.POSITIVE_INFINITY;
        for (var entry : directory) {
            WorkerStatus status = entry.getValue().getStatus();
            if (group != null && !group.equals(status.topologySnapshot().group())) {
                continue;
            }

            var engine = status.committedEngineObservation();
            String dpMaster = engine.dpMasterAddress();
            if (dpMaster != null) {
                long dpSize = engine.dpSize();
                if (dpGroups == null) {
                    dpGroups = dpSize > 1 && lastSeenDpSize > 1
                            ? HashMap.newHashMap((int) Math.max(16L, workerCount / lastSeenDpSize))
                            : new HashMap<>();
                }
                dpGroups.computeIfAbsent(dpMaster, ignored -> new DpGroupMembers()).add(dpSize, engine.dpRank());
                lastSeenDpSize = dpSize;
            }

            lowestCost = collectCandidate(entry, engine, requirement, emptyWorkers, lowestCostWorkers, lowestCost);
        }

        if (dpGroups != null) {
            boolean hadLowestCostWorkers = !lowestCostWorkers.isEmpty();
            removeIncompleteDpWorkers(emptyWorkers, dpGroups);
            removeIncompleteDpWorkers(lowestCostWorkers, dpGroups);
            if (hadLowestCostWorkers && emptyWorkers.isEmpty() && lowestCostWorkers.isEmpty()) {
                return selectForDpFallBack(context, group, directory, dpGroups, emptyWorkers, lowestCostWorkers,
                        requirement);
            }
        }
        return chooseFromCandidates(context, emptyWorkers, lowestCostWorkers, lowestCost, requirement);
    }

    private SelectedDecode selectForDpFallBack(BalanceContext context,
                                               String group,
                                               List<Map.Entry<String, DecodeEndpoint>> directory,
                                               Map<String, DpGroupMembers> dpGroups,
                                               List<Map.Entry<String, DecodeEndpoint>> emptyWorkers,
                                               List<Map.Entry<String, DecodeEndpoint>> lowestCostWorkers,
                                               DecodeRequirement requirement) {
        double lowestCost = Double.POSITIVE_INFINITY;
        for (var entry : directory) {
            WorkerStatus status = entry.getValue().getStatus();
            if (group != null && !group.equals(status.topologySnapshot().group())) {
                continue;
            }
            var engine = status.committedEngineObservation();
            if (hasIncompleteDpGroup(dpGroups, engine.dpMasterAddress())) {
                continue;
            }

            lowestCost = collectCandidate(entry, engine, requirement, emptyWorkers, lowestCostWorkers, lowestCost);
        }
        return chooseFromCandidates(context, emptyWorkers, lowestCostWorkers, lowestCost, requirement);
    }

    private double collectCandidate(Map.Entry<String, DecodeEndpoint> entry,
                                    WorkerStatus.EngineObservation engine,
                                    DecodeRequirement requirement,
                                    List<Map.Entry<String, DecodeEndpoint>> emptyWorkers,
                                    List<Map.Entry<String, DecodeEndpoint>> lowestCostWorkers,
                                    double lowestCost) {
        DecodeEndpoint endpoint = entry.getValue();
        if (endpoint.isEmptyForSelection(engine)) {
            emptyWorkers.add(entry);
            lastEmptyWorkerPeakSize = Math.max(lastEmptyWorkerPeakSize, emptyWorkers.size());
            return lowestCost;
        }

        double cost = requirement.costFormula()
                .evaluate(endpoint.totalLoad(), requirement.capacity().maxEngineRequests(),
                        endpoint.placementKvUsed(engine), Math.max(0L, engine.totalKvCacheTokens()));
        // Retain invalid scores until DP filtering can distinguish them from no routable workers.
        if (!Double.isFinite(cost)) {
            cost = Double.POSITIVE_INFINITY;
        }
        if (cost < lowestCost) {
            lowestCostWorkers.clear();
            lowestCost = cost;
        }
        if (cost == lowestCost) {
            lowestCostWorkers.add(entry);
            lastLowestCostWorkerPeakSize = Math.max(lastLowestCostWorkerPeakSize, lowestCostWorkers.size());
        }
        return lowestCost;
    }

    private static SelectedDecode chooseFromCandidates(BalanceContext context,
                                                       List<Map.Entry<String, DecodeEndpoint>> emptyWorkers,
                                                       List<Map.Entry<String, DecodeEndpoint>> lowestCostWorkers,
                                                       double lowestCost,
                                                       DecodeRequirement requirement) {
        if (!emptyWorkers.isEmpty()) {
            return choose(context, emptyWorkers, true);
        }
        if (lowestCostWorkers.isEmpty()) {
            return null;
        }
        if (!Double.isFinite(lowestCost)) {
            throw new IllegalStateException(
                    "Decode cost formula produced no finite score: " + requirement.costFormula().expression());
        }
        return choose(context, lowestCostWorkers, false);
    }

    private static SelectedDecode choose(BalanceContext context,
                                         List<Map.Entry<String, DecodeEndpoint>> workers,
                                         boolean selectedAsEmpty) {
        int count = workers.size();
        int index = count == 1 ? 0 : ThreadLocalRandom.current().nextInt(count);
        String reason = selectedAsEmpty ? "DECODE_EMPTY_WORKER" : "DECODE_MIN_COST";
        context.recordSelectionReason(RoleType.DECODE, reason);
        return new SelectedDecode(workers.get(index).getValue(), selectedAsEmpty);
    }

    private static void removeIncompleteDpWorkers(List<Map.Entry<String, DecodeEndpoint>> workers,
                                                  Map<String, DpGroupMembers> dpGroups) {
        workers.removeIf(entry -> hasIncompleteDpGroup(dpGroups,
                entry.getValue().getStatus().committedEngineObservation().dpMasterAddress()));
    }

    private static boolean hasIncompleteDpGroup(Map<String, DpGroupMembers> dpGroups, String dpMaster) {
        if (dpMaster == null) {
            return false;
        }
        DpGroupMembers members = dpGroups.get(dpMaster);
        return members == null || !members.hasAllRanks();
    }
}
