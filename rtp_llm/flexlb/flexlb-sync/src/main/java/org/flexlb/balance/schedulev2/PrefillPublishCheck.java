package org.flexlb.balance.schedulev2;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;

/**
 * 检查选中的 Prefill 是否仍可用，并管理发布名额。
 */
public final class PrefillPublishCheck {

    private final EndpointRegistry endpoints;

    private final PrefillQueueRegistry queues;

    public PrefillPublishCheck(EndpointRegistry endpoints, PrefillQueueRegistry queues) {
        this.endpoints = endpoints;
        this.queues = queues;
    }

    public QueueBlockReason tryAcquire(Request request, int maxActiveTasks) {
        PrefillQueue queue = queues.find(request);
        return queue == null ? QueueBlockReason.ENDPOINT_RETIRED : queue.tryAcquirePublishSlot(request, maxActiveTasks);
    }

    public void release(Request request) {
        PrefillQueue queue = queues.find(request);
        if (queue != null) {
            queue.releasePublishSlot(request);
        }
    }

    public boolean isAvailable(Request request) {
        PrefillEndpoint endpoint = request.selectedPrefill().endpoint();
        return !endpoint.isRetired() && endpoints.get(endpoint.getStatus().getRole(), endpoint.ipPort()) == endpoint;
    }
}
