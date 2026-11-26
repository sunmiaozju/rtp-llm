package org.flexlb.config;

/**
 * Configures local cache matching by polling each discovered worker's {@code GetCacheStatus}.
 * Supported for deployments with a pipeline parallel size of 1.
 */
public final class LocalSyncCacheMatchingConfig implements CacheMatchingConfig {

    @Override
    public CacheMatchMode getType() {
        return CacheMatchMode.LOCAL_SYNC;
    }
}
