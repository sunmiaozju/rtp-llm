package org.flexlb.cache.match.localsync;

import java.util.List;
import java.util.Set;

/**
 * Cached SWA pages and window geometry for one worker.
 */
record SwaCacheSnapshot(Set<Long> fullKeys, Set<Long> cachedKeys, long windowTokens, long blockSize) {

    SwaCacheSnapshot {
        fullKeys = Set.copyOf(fullKeys);
        cachedKeys = Set.copyOf(cachedKeys);
        if (windowTokens <= 0 || blockSize <= 0) {
            throw new IllegalArgumentException("SWA window and block size must be positive");
        }
    }

    /**
     * Return the furthest Full prefix whose SWA tail is available.
     * A missing SWA page invalidates the boundary until a complete subsequent
     * window has been found, matching the engine's SWA prefix validator.
     */
    int matchingBlocks(List<Long> keys, int fullMatchBlocks) {
        long windowBlocks = 1 + (windowTokens - 1) / blockSize;
        long consecutiveSwaBlocks = windowBlocks;
        int bestMatch = 0;
        for (int index = 0; index < fullMatchBlocks; index++) {
            if (!fullKeys.contains(keys.get(index))) {
                break;
            }
            if (cachedKeys.contains(keys.get(index))) {
                consecutiveSwaBlocks = Math.min(windowBlocks, consecutiveSwaBlocks + 1);
            } else {
                consecutiveSwaBlocks = 0;
            }
            if (consecutiveSwaBlocks >= windowBlocks) {
                bestMatch = index + 1;
            }
        }
        return bestMatch;
    }
}
