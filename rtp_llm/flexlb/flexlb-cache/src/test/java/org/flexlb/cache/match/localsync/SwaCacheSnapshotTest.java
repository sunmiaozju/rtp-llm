package org.flexlb.cache.match.localsync;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SwaCacheSnapshotTest {

    @Test
    void missingSwaPagesRequireACompleteLaterWindow() {
        List<Long> keys = List.of(1L, 2L, 3L, 4L, 5L, 6L);
        Set<Long> fullKeys = Set.copyOf(keys);
        SwaCacheSnapshot snapshot = new SwaCacheSnapshot(fullKeys, Set.of(1L, 2L, 5L, 6L), 256, 128);

        assertEquals(2, snapshot.matchingBlocks(keys, 5));
        assertEquals(6, snapshot.matchingBlocks(keys, 6));
        assertEquals(0, new SwaCacheSnapshot(fullKeys, Set.of(), 256, 128).matchingBlocks(keys, 6));
        assertEquals(2, new SwaCacheSnapshot(Set.of(1L, 2L, 3L, 4L, 5L), Set.of(1L, 2L, 5L, 6L), 256, 128)
                .matchingBlocks(keys, 6));
    }
}
