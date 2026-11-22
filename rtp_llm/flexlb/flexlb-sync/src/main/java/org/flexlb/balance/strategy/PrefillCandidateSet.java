package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.dao.pv.RoutingDecision;

import java.util.Arrays;

/** Reusable columns for modeled Prefill candidates from one fleet snapshot. */
final class PrefillCandidateSet {

    private static final int INITIAL_CAPACITY = 16;
    private String[] endpointAddresses = new String[0];
    private PrefillEndpoint[] endpoints = new PrefillEndpoint[0];
    private long[] estimatedTtftMs = new long[0];
    private long[] incomingPrefillMs = new long[0];
    private long[] cacheHitTokens = new long[0];
    private long[] routingCacheMatchTokens = new long[0];
    private long[] localHitTokens = new long[0];
    private long[] ownershipVersions = new long[0];
    private int size;
    long minimumTtftRank;
    long maxLocalHitTokensAtMinTtft;
    boolean hasKnownTtft;
    long maximumCacheHit;
    long maximumRoutingCacheMatchTokens;
    private String affinityDecision;
    private RoutingDecision.CacheAffinity cacheAffinity;

    void reset(int expectedCapacity) {
        Arrays.fill(endpointAddresses, 0, size, null);
        Arrays.fill(endpoints, 0, size, null);
        ensureCapacityForReset(expectedCapacity);
        size = 0;
        minimumTtftRank = Long.MAX_VALUE;
        maxLocalHitTokensAtMinTtft = 0L;
        hasKnownTtft = false;
        maximumCacheHit = 0L;
        maximumRoutingCacheMatchTokens = 0L;
        affinityDecision = null;
        cacheAffinity = null;
    }

    void addCandidate(String address,
                      PrefillEndpoint endpoint,
                      long estimatedMs,
                      long incomingMs,
                      long effectiveHitTokens,
                      long routingHitTokens,
                      long localHitTokens,
                      long ownershipVersion) {
        endpointAddresses[size] = address;
        endpoints[size] = endpoint;
        estimatedTtftMs[size] = estimatedMs;
        incomingPrefillMs[size] = incomingMs;
        cacheHitTokens[size] = effectiveHitTokens;
        routingCacheMatchTokens[size] = routingHitTokens;
        this.localHitTokens[size] = localHitTokens;
        ownershipVersions[size] = ownershipVersion;
        if (estimatedMs >= 0L) {
            if (!hasKnownTtft || estimatedMs < minimumTtftRank) {
                minimumTtftRank = estimatedMs;
                maxLocalHitTokensAtMinTtft = localHitTokens;
            } else if (estimatedMs == minimumTtftRank) {
                maxLocalHitTokensAtMinTtft = Math.max(maxLocalHitTokensAtMinTtft, localHitTokens);
            }
            hasKnownTtft = true;
        }
        maximumCacheHit = Math.max(maximumCacheHit, cacheHitTokens[size]);
        maximumRoutingCacheMatchTokens = Math.max(
                maximumRoutingCacheMatchTokens, routingCacheMatchTokens[size]);
        size++;
    }

    String affinityDecision() { return affinityDecision; }
    void recordAffinityDecision(String decision) { affinityDecision = decision; }
    RoutingDecision.CacheAffinity cacheAffinity() {
        return cacheAffinity;
    }

    void recordCacheAffinity(RoutingDecision.CacheAffinity details) {
        cacheAffinity = details;
    }
    PrefillEndpoint endpoint(int index) { return endpoints[index]; }
    String endpointAddress(int index) { return endpointAddresses[index]; }
    long cacheHit(int index) { return cacheHitTokens[index]; }

    long localHitTokens(int index) {
        return localHitTokens[index];
    }

    long ttftRank(int index) { return estimatedTtftMs[index] < 0L ? Long.MAX_VALUE : estimatedTtftMs[index]; }
    long estimatedTtftMs(int index) { return estimatedTtftMs[index]; }
    long prefillMs(int index) { return incomingPrefillMs[index]; }
    long routingCacheMatchTokens(int index) { return routingCacheMatchTokens[index]; }
    long ownershipVersion(int index) { return ownershipVersions[index]; }
    int size() { return size; }

    private void ensureCapacityForReset(int expectedCapacity) {
        if (endpoints.length >= expectedCapacity) {
            return;
        }
        int capacity = Math.max(expectedCapacity,
                Math.max(INITIAL_CAPACITY, endpoints.length << 1));
        endpointAddresses = new String[capacity];
        endpoints = new PrefillEndpoint[capacity];
        estimatedTtftMs = new long[capacity];
        incomingPrefillMs = new long[capacity];
        cacheHitTokens = new long[capacity];
        routingCacheMatchTokens = new long[capacity];
        localHitTokens = new long[capacity];
        ownershipVersions = new long[capacity];
    }

}
