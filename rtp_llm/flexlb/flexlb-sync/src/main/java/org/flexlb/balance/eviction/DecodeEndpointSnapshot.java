package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.DecodeEndpoint.DecodeRequestView;
import org.flexlb.dao.route.CapacityLimit;
import org.flexlb.dao.route.CapacityUsage;
import org.flexlb.dao.route.DecodeRequirement;

import java.util.List;

/** Victims and placement occupancy captured under one endpoint admission lock. */
public record DecodeEndpointSnapshot(DecodeEndpoint endpoint,
                                     String endpointId,
                                     CapacityLimit capacity,
                                     CapacityUsage usage,
                                     List<DecodeRequestView> reserved,
                                     List<DecodeRequestView> accepted,
                                     List<DecodeRequestView> running) {

    public static DecodeEndpointSnapshot captureForReturn(DecodeEndpoint endpoint, DecodeRequirement requirement) {
        var candidates = endpoint.collectReturnPreemptionCandidates(requirement);
        if (candidates == null) {
            return null;
        }
        return new DecodeEndpointSnapshot(endpoint, endpoint.ipPort(), requirement.capacity(), candidates.usage(),
                candidates.reserved(), candidates.accepted(), candidates.running());
    }

}
