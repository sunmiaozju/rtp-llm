package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PrefillCandidateSetTest {

    @Test
    void tracksLocalHitsOnlyWithinTheLowestKnownTtft() {
        PrefillCandidateSet candidates = new PrefillCandidateSet(5);
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);

        candidates.addCandidate(new PrefillCandidateInfo("unknown", endpoint, -1L, 0L, 1200L, 900L, 0L));
        assertFalse(candidates.hasKnownTtft);
        assertEquals(Long.MAX_VALUE, candidates.minimumTtftRank);
        assertEquals(0L, candidates.maxLocalHitTokensAtMinTtft);
        candidates.addCandidate(new PrefillCandidateInfo("first", endpoint, 7L, 0L, 512L, 512L, 0L));
        candidates.addCandidate(new PrefillCandidateInfo("faster", endpoint, 5L, 0L, 128L, 128L, 0L));
        assertTrue(candidates.hasKnownTtft);
        assertEquals(5L, candidates.minimumTtftRank);
        assertEquals(128L, candidates.maxLocalHitTokensAtMinTtft);

        candidates.addCandidate(new PrefillCandidateInfo("equally-fast", endpoint, 5L, 0L, 256L, 256L, 0L));
        candidates.addCandidate(new PrefillCandidateInfo("slower", endpoint, 6L, 0L, 1024L, 1024L, 0L));
        assertEquals(256L, candidates.maxLocalHitTokensAtMinTtft);
        assertEquals(1200L, candidates.maxEffectiveCacheHitTokens);
    }
}
