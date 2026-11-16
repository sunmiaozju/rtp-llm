package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.estimate.PrefillTtftEstimate;
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
            assertEquals(0L, candidates.maximumCacheHit);
            assertEquals(0L, candidates.maximumRoutingCacheMatchTokens);

            for (int index = 0; index < candidateCount; index++) {
                candidates.addCandidate("worker-" + index, endpoint,
                        new PrefillTtftEstimate.Candidate(PrefillTtftEstimate.Candidate.State.MODELED, 100L + index,
                                20L + index, PrefillTtftEstimate.Candidate.InitialHeadDisposition.NONE, "", null,
                                10L + index, 15L + index),
                        10L + index, 30L + index);
            }

            assertEquals(candidateCount, candidates.size());
            for (int index = 0; index < candidateCount; index++) {
                assertEquals("worker-" + index, candidates.endpointAddress(index));
                assertSame(endpoint, candidates.endpoint(index));
                assertEquals(100L + index, candidates.estimatedTtftMs(index));
                assertEquals(20L + index, candidates.prefillMs(index));
                assertEquals(10L + index, candidates.cacheHit(index));
                assertEquals(15L + index, candidates.routingCacheMatchTokens(index));
                assertEquals(30L + index, candidates.ownershipVersion(index));
            }
        }
    }
}
