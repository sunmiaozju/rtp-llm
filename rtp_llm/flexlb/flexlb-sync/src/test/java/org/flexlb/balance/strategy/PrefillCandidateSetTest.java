package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

class PrefillCandidateSetTest {

    @Test
    void resetSupportsLargerCandidateSetsAndReusesExistingCapacity() {
        PrefillCandidateSet candidates = new PrefillCandidateSet();
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);

        for (int candidateCount : new int[]{17, 33, 2, 65, 0}) {
            candidates.reset(candidateCount);
            assertEquals(0, candidates.size());
            assertFalse(candidates.hasKnownTtft);
            assertEquals(Long.MAX_VALUE, candidates.minimumTtftRank);
            assertEquals(0L, candidates.maxLocalHitTokensAtMinTtft);
            assertEquals(0L, candidates.maximumCacheHit);
            assertEquals(0L, candidates.maximumRoutingCacheMatchTokens);

            for (int index = 0; index < candidateCount; index++) {
                candidates.addCandidate("worker-" + index, endpoint, 100L + index, 20L + index, 10L + index,
                        15L + index, 5L + index, 30L + index);
            }

            assertEquals(candidateCount, candidates.size());
            for (int index = 0; index < candidateCount; index++) {
                assertEquals("worker-" + index, candidates.endpointAddress(index));
                assertSame(endpoint, candidates.endpoint(index));
                assertEquals(100L + index, candidates.estimatedTtftMs(index));
                assertEquals(20L + index, candidates.prefillMs(index));
                assertEquals(10L + index, candidates.cacheHit(index));
                assertEquals(15L + index, candidates.routingCacheMatchTokens(index));
                assertEquals(5L + index, candidates.localHitTokens(index));
                assertEquals(30L + index, candidates.ownershipVersion(index));
            }
        }
    }

    @Test
    void tracksLocalHitsOnlyWithinTheLowestKnownTtft() {
        PrefillCandidateSet candidates = new PrefillCandidateSet();
        candidates.reset(5);
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);

        candidates.addCandidate("unknown", endpoint, -1L, 0L, 900L, 900L, 900L, 0L);
        candidates.addCandidate("first", endpoint, 7L, 0L, 512L, 512L, 512L, 0L);
        candidates.addCandidate("faster", endpoint, 5L, 0L, 128L, 128L, 128L, 0L);
        assertEquals(5L, candidates.minimumTtftRank);
        assertEquals(128L, candidates.maxLocalHitTokensAtMinTtft);

        candidates.addCandidate("equally-fast", endpoint, 5L, 0L, 256L, 256L, 256L, 0L);
        candidates.addCandidate("slower", endpoint, 6L, 0L, 1024L, 1024L, 1024L, 0L);
        assertEquals(256L, candidates.maxLocalHitTokensAtMinTtft);
    }
}
