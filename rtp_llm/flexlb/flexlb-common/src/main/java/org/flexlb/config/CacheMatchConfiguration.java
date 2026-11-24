package org.flexlb.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.flexlb.dao.route.KvcmConfig;
import org.flexlb.dao.route.ServiceRoute;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Centralized cache-matching configuration derived from the model service route.
 */
@Slf4j
@Component
public class CacheMatchConfiguration {

    @Getter
    private final ServiceRoute serviceRoute;
    // 本地备用索引的参数和自动切换策略在启动时确定，不随后续配置更新而改变。
    @Getter
    private final LocalStandbyConfig localStandbyConfig;
    private volatile CacheMatchingConfig cacheMatching;

    @Autowired
    public CacheMatchConfiguration(
            ModelMetaConfig modelMetaConfig,
            ConfigService configService) {
        this(modelMetaConfig, configService.loadBalanceConfig());
        if (isKvcmEnabled()) {
            configService.addUpdateListener(this::applyRuntimeConfigUpdate);
        }
    }

    public CacheMatchConfiguration(ModelMetaConfig modelMetaConfig, FlexlbConfig flexlbConfig) {
        this.serviceRoute = modelMetaConfig.getServiceRoute();
        this.cacheMatching = flexlbConfig.getCacheMatching();
        if (getConfiguredMode() == CacheMatchMode.KVCM && getKvcmTopology() == null) {
            throw new IllegalStateException(
                    "FLEXLB_CONFIG cacheMatching.type=KVCM requires MODEL_SERVICE_CONFIG kvcm topology");
        }
        this.localStandbyConfig = flexlbConfig.isKvcmAndStandbyEnabled()
                ? flexlbConfig.getKvcmAndStandbyConfig().getLocalStandby() : null;
        logInitialization();
    }

    /**
     * Publishes the KVCM runtime parameters of a configuration update. The volatile write makes
     * the freshly deserialized instance safely visible to routing threads.
     */
    private void applyRuntimeConfigUpdate(FlexlbConfig config) {
        if (config.getCacheMatching().getType() == CacheMatchMode.LOCAL_SYNC) {
            log.warn("Ignored runtime cacheMatching.type change; activating another cache match "
                    + "mode requires a restart");
            return;
        }
        KvcmCacheMatchingConfig clientSettings = config.getKvcmAndStandbyConfig();
        cacheMatching = clientSettings;
        log.info(
                "Applied KVCM cache matching configuration update: requestTimeoutMs={}, queryTimeoutMs={}, "
                        + "heartbeatFailureThreshold={}, queryFailureThreshold={}, maxQueryRetryCount={}, "
                        + "recoverySuccessThreshold={}, topKHostCount={}, backendTypes={}, medium={}",
                clientSettings.getRequestTimeoutMs(), clientSettings.getQueryTimeoutMs(),
                clientSettings.getHeartbeatFailureThreshold(), clientSettings.getQueryFailureThreshold(),
                clientSettings.getMaxQueryRetryCount(), clientSettings.getRecoverySuccessThreshold(),
                clientSettings.getTopKHostCount(), clientSettings.getBackendTypes(), clientSettings.getMedium());
    }

    public CacheMatchMode getConfiguredMode() {
        return cacheMatching.getType();
    }

    /**
     * 返回启动时从 {@code MODEL_SERVICE_CONFIG.kvcm} 读取的地址、服务发现配置和命名空间。
     * 未配置 KVCM 时返回 {@code null}。
     */
    public KvcmConfig getKvcmTopology() {
        return serviceRoute.getKvcm();
    }

    public boolean isLocalSyncEnabled() {
        return getConfiguredMode() == CacheMatchMode.LOCAL_SYNC;
    }

    public boolean isLocalStandbyEnabled() {
        return localStandbyConfig != null;
    }

    public boolean isKvcmEnabled() {
        return getKvcmClientSettings() != null;
    }

    public boolean isAutoSwitchEnabled() {
        return isKvcmEnabled() && localStandbyConfig.isAutoSwitch();
    }

    /**
     * 返回当前 {@code FLEXLB_CONFIG.cacheMatching} 中用于 KVCM 查询和健康探测的参数。
     * 热更新对后续查询生效，已发起的查询在重试时仍使用原参数。未启用 KVCM 客户端时返回 {@code null}。
     */
    public KvcmCacheMatchingConfig getKvcmClientSettings() {
        return getKvcmTopology() != null && cacheMatching instanceof KvcmCacheMatchingConfig kvcm ? kvcm : null;
    }

    private void logInitialization() {
        log.info(
                "Cache match configuration initialized: configuredMode={}, kvcmEnabled={}, autoSwitchEnabled={}",
                getConfiguredMode(), isKvcmEnabled(), isAutoSwitchEnabled());
        if (isKvcmEnabled()) {
            KvcmConfig topology = getKvcmTopology();
            KvcmCacheMatchingConfig clientSettings = getKvcmClientSettings();
            log.info(
                    "KVCM cache matching configuration: address={}, namespace={}, "
                            + "requestTimeoutMs={}, queryTimeoutMs={}, leaderRefreshIntervalMs={}, "
                            + "heartbeatFailureThreshold={}, queryFailureThreshold={}, maxQueryRetryCount={}, "
                            + "recoverySuccessThreshold={}, topKHostCount={}, backendTypes={}, medium={}",
                    topology.getAddress(), topology.getNamespace(), clientSettings.getRequestTimeoutMs(),
                    clientSettings.getQueryTimeoutMs(), clientSettings.getLeaderRefreshIntervalMs(),
                    clientSettings.getHeartbeatFailureThreshold(), clientSettings.getQueryFailureThreshold(),
                    clientSettings.getMaxQueryRetryCount(), clientSettings.getRecoverySuccessThreshold(),
                    clientSettings.getTopKHostCount(), clientSettings.getBackendTypes(), clientSettings.getMedium());
        }
        if (isLocalStandbyEnabled()) {
            log.info("Local standby cache configuration: autoSwitch={}, "
                            + "ttlMs={}, minimumTtlMs={}, ttlReductionStartRatio={}, "
                            + "maximumEntries={}, capacityMultiplier={}, asyncQueueCapacity={}",
                    localStandbyConfig.isAutoSwitch(),
                    localStandbyConfig.getTtlMs(),
                    localStandbyConfig.getMinimumTtlMs(),
                    localStandbyConfig.getTtlReductionStartRatio(),
                    localStandbyConfig.getMaximumEntries(),
                    localStandbyConfig.getCapacityMultiplier(),
                    localStandbyConfig.getAsyncQueueCapacity());
        }
    }
}
