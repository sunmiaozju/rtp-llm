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
        var fullWorkers = new ArrayList<Map.Entry<String, DecodeEndpoint>>();
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

            lowestCost = collectCandidate(entry, engine, requirement, emptyWorkers, lowestCostWorkers, fullWorkers,
                    lowestCost);
        }

        if (dpGroups != null) {
            boolean hadLowestCostWorkers = !lowestCostWorkers.isEmpty();
            removeIncompleteDpWorkers(emptyWorkers, dpGroups);
            removeIncompleteDpWorkers(lowestCostWorkers, dpGroups);
            if (emptyWorkers.isEmpty() && lowestCostWorkers.isEmpty()) {
                if (hadLowestCostWorkers) {
                    // 最低分节点所在的 DP 组不完整，完整组里可能还有能接单的节点。
                    return selectForDpFallBack(context, group, directory, dpGroups, emptyWorkers, lowestCostWorkers,
                            requirement);
                }
                removeIncompleteDpWorkers(fullWorkers, dpGroups);
            }
        }
        return chooseFromCandidates(context, emptyWorkers, lowestCostWorkers, fullWorkers, lowestCost, requirement);
    }

    private SelectedDecode selectForDpFallBack(BalanceContext context,
                                               String group,
                                               List<Map.Entry<String, DecodeEndpoint>> directory,
                                               Map<String, DpGroupMembers> dpGroups,
                                               List<Map.Entry<String, DecodeEndpoint>> emptyWorkers,
                                               List<Map.Entry<String, DecodeEndpoint>> lowestCostWorkers,
                                               DecodeRequirement requirement) {
        var fullWorkers = new ArrayList<Map.Entry<String, DecodeEndpoint>>();
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

            lowestCost = collectCandidate(entry, engine, requirement, emptyWorkers, lowestCostWorkers, fullWorkers,
                    lowestCost);
        }
        return chooseFromCandidates(context, emptyWorkers, lowestCostWorkers, fullWorkers, lowestCost, requirement);
    }

    private double collectCandidate(Map.Entry<String, DecodeEndpoint> entry,
                                    WorkerStatus.EngineObservation engine,
                                    DecodeRequirement requirement,
                                    List<Map.Entry<String, DecodeEndpoint>> emptyWorkers,
                                    List<Map.Entry<String, DecodeEndpoint>> lowestCostWorkers,
                                    List<Map.Entry<String, DecodeEndpoint>> fullWorkers,
                                    double lowestCost) {
        DecodeEndpoint endpoint = entry.getValue();
        if (endpoint.isEmptyForSelection(engine)) {
            emptyWorkers.add(entry);
            lastEmptyWorkerPeakSize = Math.max(lastEmptyWorkerPeakSize, emptyWorkers.size());
            return lowestCost;
        }

        var usage = endpoint.placementUsageForSelection(engine);
        // 放不下这笔请求的节点不参与打分，只留作全满时的抢占候选。
        if (!requirement.capacity()
                .evaluate(usage, requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens())
                .hasCapacity()) {
            fullWorkers.add(entry);
            return lowestCost;
        }
        double cost = requirement.costFormula()
                .evaluate(usage.occupiedRequests(), requirement.capacity().maxEngineRequests(),
                        usage.kvBudgetUsedTokens(), usage.totalKvTokens());
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
                                                       List<Map.Entry<String, DecodeEndpoint>> fullWorkers,
                                                       double lowestCost,
                                                       DecodeRequirement requirement) {
        if (!emptyWorkers.isEmpty()) {
            return choose(context, emptyWorkers, true);
        }
        if (lowestCostWorkers.isEmpty()) {
            if (fullWorkers.isEmpty()) {
                return null;
            }
            context.recordSelectionReason(RoleType.DECODE, "DECODE_ALL_CANDIDATES_FULL");
            return SelectedDecode.whenAllCandidatesFull(chooseRandomWorker(fullWorkers));
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
        String reason = selectedAsEmpty ? "DECODE_EMPTY_WORKER" : "DECODE_MIN_COST";
        context.recordSelectionReason(RoleType.DECODE, reason);
        return new SelectedDecode(chooseRandomWorker(workers), selectedAsEmpty);
    }

    private static DecodeEndpoint chooseRandomWorker(List<Map.Entry<String, DecodeEndpoint>> workers) {
        int index = workers.size() == 1 ? 0 : ThreadLocalRandom.current().nextInt(workers.size());
        return workers.get(index).getValue();
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
