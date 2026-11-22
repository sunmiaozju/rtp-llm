package org.flexlb.sync.status;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusProvider;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.ReadOnlyListFilter;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Process-local owner of discovered worker generations and routable endpoint
 * generations.
 *
 * <p>A status entry describes the generation currently being polled. Routing
 * authority always comes from the matching endpoint generation in
 * {@link EndpointRegistry}; callers must take a hold before using it. Keeping
 * both views behind this directory makes disagreement visible instead of
 * hiding it behind a derived capacity value.</p>
 */
@Component
public final class WorkerDirectory implements WorkerStatusProvider {

    private final Map<RoleType, ConcurrentHashMap<String, WorkerStatus>> statusesByRole = new EnumMap<>(RoleType.class);
    private final EndpointRegistry endpointRegistry;

    public WorkerDirectory(EndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
        for (RoleType role : RoleType.values()) {
            statusesByRole.put(role, new ConcurrentHashMap<>());
        }
    }

    /**
     * Live view of discovered generations for one role; callers must not modify it.
     */
    public Map<String, WorkerStatus> getStatusBy(RoleType role) {
        if (role == null) {
            return Map.of();
        }
        return statusesByRole.get(role);
    }

    @Override
    public Collection<WorkerStatus> getWorkerStatuses(RoleType role, String group) {
        if (role == null) {
            return List.of();
        }
        Collection<WorkerStatus> statuses = getStatusBy(role).values();
        if (group == null) {
            return statuses;
        }
        return statuses.stream()
                .filter(status -> group.equals(status.getGroup()))
                .toList();
    }

    /** Identity check used by asynchronous callbacks under the status lock. */
    public boolean isCurrentStatus(
            RoleType role, String address, WorkerStatus expected) {
        return expected != null && role != null && address != null
                && statusesByRole.get(role).get(address) == expected;
    }

    /**
     * Atomically install one newly discovered generation. Discovery publishes
     * status identity only; its endpoint remains absent until the first valid
     * WorkerStatus response commits.
     */
    public WorkerStatus currentOrDiscover(RoleType role, String address, Supplier<WorkerStatus> discoveredFactory) {
        return statusesByRole.get(role).computeIfAbsent(address, ignored -> {
            WorkerStatus discovered = discoveredFactory.get();
            if (discovered.getRole() != role
                    || !address.equals(discovered.getLogicalIpPort())) {
                throw new IllegalArgumentException(
                        "Discovered WorkerStatus identity does not match directory key");
            }
            return discovered;
        });
    }

    public int discoveredCount(RoleType role) {
        return role == null ? 0 : statusesByRole.get(role).size();
    }

    public int discoveredCount() {
        int total = 0;
        for (Map<String, WorkerStatus> statuses : statusesByRole.values()) {
            total += statuses.size();
        }
        return total;
    }

    /** Capacity means currently published routing capacity, not discovery. */
    public int routingCapacity(RoleType role) {
        return role == null ? 0 : endpointRegistry.getEndpointCount(role);
    }

    /** Exact endpoint for a discovered generation, or {@code null}. */
    public WorkerEndpoint getEndpoint(RoleType role, String address, WorkerStatus workerStatus) {
        return endpointRegistry.get(role, address, workerStatus);
    }

    /** Publish the first endpoint for one exact discovered generation. */
    public EndpointRegistry.EndpointPublication publishPreparedEndpoint(
            String address,
            WorkerStatus status,
            WorkerStatus.PreparedStatus prepared) {
        return endpointRegistry.publishPreparedEndpoint(
                address, status, prepared);
    }

    /**
     * Close and detach the exact endpoint gate before publishing RETIRING.
     * The caller must hold the matching {@link WorkerStatus#lock}.
     */
    public EndpointRegistry.DetachedGeneration beginRetirement(
            RoleType role,
            String address,
            WorkerStatus status) {
        status.requireGenerationLock();
        status.requireActiveGeneration();

        WorkerEndpoint expected = endpointRegistry.get(
                role, address, status);
        EndpointRegistry.DetachedGeneration detached =
                endpointRegistry.detachAndBeginRetirement(
                        role, address, status);
        if ((detached == null) != (expected == null)
                || detached != null && !detached.ownsEndpoint(expected)) {
            throw new IllegalStateException(
                    "Exact endpoint detach invariant failed for "
                            + address + "#" + status.getGenerationId());
        }
        if (detached == null
                && !status.beginRetirementAfterEndpointGateClosed()) {
            throw new IllegalStateException(
                    "WorkerStatus generation changed while its lock was held: "
                            + address + "#" + status.getGenerationId());
        }
        return detached;
    }

    /**
     * Retire the remaining members of the same role and DP master before awaiting
     * any endpoint cleanup. The caller must release the source status lock first.
     */
    public void completeRetirement(RoleType role,
                                   String address,
                                   WorkerStatus status,
                                   EndpointRegistry.DetachedGeneration detached,
                                   CacheAwareService cacheAwareService,
                                   Logger logger) {
        Map<WorkerStatus, EndpointRegistry.DetachedGeneration> dpGroupMap = new HashMap<>();
        String masterAddress = status.committedEngineObservation().dpMasterAddress();
        if (masterAddress != null) {
            for (WorkerStatus member : getStatusBy(role).values()) {
                member.lock.lock();
                try {
                    if (member.isActiveGeneration()
                            && masterAddress.equals(member.committedEngineObservation().dpMasterAddress())) {
                        dpGroupMap.put(member, beginRetirement(role, member.getLogicalIpPort(), member));
                    }
                } finally {
                    member.lock.unlock();
                }
            }
        }

        completeWorkerRetirement(role, address, status, detached, cacheAwareService, logger);
        for (Map.Entry<WorkerStatus, EndpointRegistry.DetachedGeneration> dpRank : dpGroupMap.entrySet()) {
            WorkerStatus dpRankWorkStatus = dpRank.getKey();
            completeWorkerRetirement(role, dpRankWorkStatus.getLogicalIpPort(), dpRankWorkStatus, dpRank.getValue(),
                    cacheAwareService, logger);
        }
    }

    /**
     * Remove an exact RETIRING status only after its endpoint and cache cleanup finish.
     */
    private void completeWorkerRetirement(RoleType role,
                                          String address,
                                          WorkerStatus status,
                                          EndpointRegistry.DetachedGeneration detached,
                                          CacheAwareService cacheAwareService,
                                          Logger logger) {
        Throwable cleanupFailure = null;
        try {
            if (detached != null) {
                detached.retireAndAwait();
            }
        } catch (Throwable retirementFailure) {
            cleanupFailure = retirementFailure;
        } finally {
            finalizeRetirement(
                    role, address, status, cacheAwareService, logger);
        }
        if (cleanupFailure != null) {
            logger.error(
                    "Endpoint cleanup failed after retiring generation {} for {}",
                    status.getGenerationId(), address, cleanupFailure);
        }
    }

    private void finalizeRetirement(
            RoleType role,
            String address,
            WorkerStatus status,
            CacheAwareService cacheAwareService,
            Logger logger) {
        Map<String, WorkerStatus> statuses = statusesByRole.get(role);
        status.lock.lock();
        try {
            status.requireRetiringGeneration();
            if (statuses.get(address) != status) {
                logger.error(
                        "Status identity changed before retirement finalized for {}#{}",
                        address, status.getGenerationId());
                return;
            }
            try {
                if (role.requiresCacheKeys()) {
                    cacheAwareService.removeEngineBlockCache(address);
                }
            } catch (Throwable cacheCleanupFailure) {
                logger.error(
                        "Cache cleanup failed while retiring generation {} for {}",
                        status.getGenerationId(), address, cacheCleanupFailure);
            }
            if (!statuses.remove(address, status)) {
                logger.error(
                        "Exact status removal failed while its generation lock was held for {}#{}",
                        address, status.getGenerationId());
            }
        } catch (Throwable finalizationFailure) {
            logger.error(
                    "Status retirement finalization failed for {}#{}",
                    address, status.getGenerationId(), finalizationFailure);
        } finally {
            status.lock.unlock();
        }
    }

    /**
     * Non-owning address view for selection; callers must not modify it.
     */
    public Collection<String> endpointAddressView(RoleType role) {
        return endpointRegistry.endpointAddressView(role);
    }
    /**
     * Prefill candidates whose reported DP group has every rank available for routing.
     * The winning address must still be held and identity-checked.
     */
    public List<EndpointRegistry.PrefillRoutingEntry> getPrefillCandidates(RoleType role) {
        List<EndpointRegistry.PrefillRoutingEntry> directory = endpointRegistry.prefillRoutingSnapshot(role);
        if (directory.isEmpty()) {
            return directory;
        }
        Map<String, DpGroupMembers> dpGroups = new HashMap<>();
        for (EndpointRegistry.PrefillRoutingEntry entry : directory) {
            WorkerStatus.EngineObservation engine =
                    entry.endpoint().getStatus().committedEngineObservation();
            if (engine.dpMasterAddress() != null) {
                dpGroups.computeIfAbsent(engine.dpMasterAddress(), ignored -> new DpGroupMembers())
                        .add(engine.dpSize(), engine.dpRank());
            }
        }
        if (dpGroups.isEmpty()) {
            return directory;
        }
        ReadOnlyListFilter<EndpointRegistry.PrefillRoutingEntry> matching = new ReadOnlyListFilter<>(directory);
        for (int index = 0; index < directory.size(); index++) {
            EndpointRegistry.PrefillRoutingEntry entry = directory.get(index);
            String masterAddress = entry.endpoint().getStatus()
                    .committedEngineObservation().dpMasterAddress();
            matching.keepIf(index, masterAddress == null || dpGroups.get(masterAddress).hasAllRanks());
        }
        return matching.toList();
    }

    public WorkerEndpoint findEndpoint(RoleType role, String ipPort) {
        WorkerEndpoint endpoint = endpointRegistry.get(role, ipPort);
        return endpoint == null || endpoint.isRetired() ? null : endpoint;
    }

    /**
     * Decode candidates whose reported DP group has every rank available for routing.
     */
    public List<Map.Entry<String, DecodeEndpoint>> getDecodeCandidates(String group) {
        List<Map.Entry<String, DecodeEndpoint>> directory = endpointRegistry.decodeRoutingEntries();
        if (directory.isEmpty()) {
            return directory;
        }
        Map<String, DpGroupMembers> dpGroups = new HashMap<>();
        // 先统计完整目录的 DP ranks，再过滤流量 group，避免把同一 DP 组的其他 rank 提前排除。
        for (Map.Entry<String, DecodeEndpoint> entry : directory) {
            WorkerStatus.EngineObservation engine = entry.getValue().getStatus().committedEngineObservation();
            if (engine.dpMasterAddress() != null) {
                dpGroups.computeIfAbsent(engine.dpMasterAddress(), ignored -> new DpGroupMembers())
                        .add(engine.dpSize(), engine.dpRank());
            }
        }
        if (group == null && dpGroups.isEmpty()) {
            return directory;
        }
        var matching = new ReadOnlyListFilter<>(directory);
        for (int index = 0; index < directory.size(); index++) {
            WorkerStatus status = directory.get(index).getValue().getStatus();
            String masterAddress = status.committedEngineObservation().dpMasterAddress();
            DpGroupMembers members = dpGroups.get(masterAddress);
            matching.keepIf(index, (group == null || group.equals(status.topologySnapshot().group()))
                    && (masterAddress == null || (members != null && members.hasAllRanks())));
        }
        return matching.toList();
    }

    public DecodeEndpoint findDecodeEndpoint(Map.Entry<String, DecodeEndpoint> expected) {
        DecodeEndpoint endpoint = expected.getValue();
        return findEndpoint(RoleType.DECODE, expected.getKey()) == endpoint ? endpoint : null;
    }

}
