package org.flexlb.dao.master;

/**
 * Engine feedback comparing the routing cache-hit prediction with the actual cache hit.
 * The embedded {@link WorkerIdentity} preserves the routing identity
 * ({@code ip:port@engineIndex}).
 */
public record CacheHitFeedback(String eventType,
                               String requestId,
                               String cacheMatchSource,
                               String role,
                               String group,
                               WorkerIdentity workerIdentity,
                               String taskState,
                               long inputTokens,
                               long blockSize,
                               long predictedHitTokens,
                               boolean kvcmMatchAvailable,
                               long kvcmLocalMatchTokens,
                               long kvcmGlobalMatchTokens,
                               long kvcmBestLocalMatchTokens,
                               long actualHitTokens,
                               long deltaHitTokens) {

    /**
     * Returns the feedback target in {@code ip:port@engineIndex} format. The index identifies
     * one independently routable engine behind the physical frontend.
     */
    public String logicalWorkerId() {
        return workerIdentity.getLogicalIpPort();
    }

    public int workerPort() {
        return workerIdentity.getPort();
    }

    public int engineIndex() {
        return workerIdentity.getEngineIndex();
    }
}
