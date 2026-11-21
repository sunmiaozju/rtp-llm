package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.EncoderEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.sync.status.WorkerDirectory;
import org.springframework.stereotype.Component;

/**
 * Selects a healthy Encoder using observed requests and uncached MM work.
 */
@Component
public final class EncoderStrategy {

    private final WorkerDirectory workerDirectory;

    /**
     * Select from the current worker directory.
     */
    public EncoderStrategy(WorkerDirectory workerDirectory) {
        this.workerDirectory = workerDirectory;
    }

    /**
     * Choose by uncached MM work when the client supplies a cache estimate.
     * Older requests continue to choose by running, waiting, and locally pending count.
     * Returns null when no worker qualifies.
     */
    public EncoderEndpoint select(BalanceContext context, String group) {
        context.beginRoutingAttempt(RoleType.ENCODER);
        int maxInflightPerWorker = context.getConfig().getDispatcher().getMaxInflightPerEncoderWorker();
        EncoderEndpoint winner = null;
        boolean weighted = context.getRequest().getEncoderCacheHitLen() != null;
        long leastUncachedTokens = Long.MAX_VALUE;
        long fewestRequests = Long.MAX_VALUE;
        for (String address : workerDirectory.endpointAddressView(RoleType.ENCODER)) {
            WorkerEndpoint candidate = workerDirectory.findEndpoint(RoleType.ENCODER, address);
            if (!(candidate instanceof EncoderEndpoint endpoint)) {
                continue;
            }
            WorkerStatus status = endpoint.getStatus();
            WorkerStatus.TopologySnapshot topology = status.topologySnapshot();
            WorkerStatus.EngineObservation engine = status.committedEngineObservation();
            if (!status.isAlive() || group != null && !group.equals(topology.group())) {
                continue;
            }
            long requests = Math.max(0, engine.runningQueryLen()) + Math.max(0, engine.waitingQueryLen())
                    + endpoint.pendingEncoderRequestCount();
            if (requests >= maxInflightPerWorker) {
                continue;
            }
            long uncachedTokens = weighted ? endpoint.inflightUncachedTokenEstimate() : 0L;
            if (uncachedTokens > leastUncachedTokens
                    || uncachedTokens == leastUncachedTokens && requests >= fewestRequests) {
                continue;
            }
            winner = endpoint;
            leastUncachedTokens = uncachedTokens;
            fewestRequests = requests;
        }
        if (winner == null) {
            return null;
        }
        context.recordSelectionReason(RoleType.ENCODER,
                weighted ? "LEAST_UNCACHED_TOKENS_CONCURRENCY" : "LEAST_CONCURRENT");
        return winner;
    }
}
