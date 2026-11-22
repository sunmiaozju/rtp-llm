package org.flexlb.config;

import org.flexlb.dao.route.KvcmConfig;
import org.flexlb.dao.route.ServiceRoute;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheMatchConfigurationTest {

    @Test
    void defaultsToKvcmAndRequiresTopology() {
        FlexlbConfig config = ConfigTestFixtures.parse("{}");
        CacheMatchConfiguration configuration = new CacheMatchConfiguration(new ModelMetaConfig(route(true)), config);
        assertEquals(CacheMatchMode.KVCM, configuration.getConfiguredMode());
        assertTrue(configuration.isKvcmEnabled());
        assertTrue(configuration.isLocalStandbyEnabled());
        assertFalse(configuration.isLocalSyncEnabled());
        assertThrows(IllegalStateException.class,
                () -> new CacheMatchConfiguration(new ModelMetaConfig(route(false)), config));
    }

    @Test
    void usesExplicitLocalSync() {
        FlexlbConfig config = new FlexlbConfig();
        config.setCacheMatching(new LocalSyncCacheMatchingConfig());
        CacheMatchConfiguration configuration = new CacheMatchConfiguration(new ModelMetaConfig(route(false)), config);

        assertFalse(configuration.isKvcmEnabled());
        assertTrue(configuration.isLocalSyncEnabled());
        assertFalse(configuration.isLocalStandbyEnabled());
        assertFalse(configuration.isAutoSwitchEnabled());
        assertNull(configuration.getKvcmTopology());
        assertNull(configuration.getKvcmClientSettings());
        assertNull(configuration.getLocalStandbyConfig());
        assertEquals(CacheMatchMode.LOCAL_SYNC, configuration.getConfiguredMode());
    }

    @Test
    void usesTopologyFromModelConfigAndPolicyFromFlexlbConfig() {
        ServiceRoute route = route(true);
        FlexlbConfig flexlbConfig = kvcmConfig(false);

        CacheMatchConfiguration configuration = new CacheMatchConfiguration(new ModelMetaConfig(route), flexlbConfig);

        assertTrue(configuration.isKvcmEnabled());
        assertFalse(configuration.isLocalSyncEnabled());
        assertTrue(configuration.isLocalStandbyEnabled());
        assertSame(route, configuration.getServiceRoute());
        assertSame(route.getKvcm(), configuration.getKvcmTopology());
        assertSame(flexlbConfig.getKvcmAndStandbyConfig(), configuration.getKvcmClientSettings());
        assertSame(flexlbConfig.getKvcmAndStandbyConfig().getLocalStandby(), configuration.getLocalStandbyConfig());
        assertFalse(configuration.isAutoSwitchEnabled());
        assertEquals(CacheMatchMode.KVCM, configuration.getConfiguredMode());
    }

    @Test
    void readsAutoSwitchFromFlexlbConfig() {
        CacheMatchConfiguration configuration =
                new CacheMatchConfiguration(new ModelMetaConfig(route(true)), kvcmConfig(true));

        assertTrue(configuration.isLocalStandbyEnabled());
        assertTrue(configuration.isAutoSwitchEnabled());
    }

    @Test
    void retainsSuppliedKvcmListsAndNormalizesNullLists() {
        KvcmCacheMatchingConfig config = new KvcmCacheMatchingConfig();
        List<String> medium = new ArrayList<>(List.of("kvs"));
        List<KvcmCacheMatchingConfig.BackendType> backendTypes =
                new ArrayList<>(List.of(KvcmCacheMatchingConfig.BackendType.ST_EVENT_REPORT_L2));
        config.setMedium(medium);
        config.setBackendTypes(backendTypes);

        assertSame(medium, config.getMedium());
        assertSame(backendTypes, config.getBackendTypes());

        config.setMedium(null);
        config.setBackendTypes(null);
        assertEquals(List.of(), config.getMedium());
        assertEquals(List.of(), config.getBackendTypes());
    }

    @Test
    void rejectsKvcmModeWithoutKvcmTopology() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new CacheMatchConfiguration(new ModelMetaConfig(route(false)), kvcmConfig(true)));

        assertTrue(error.getMessage().contains(
                "cacheMatching.type=KVCM requires MODEL_SERVICE_CONFIG kvcm topology"));
    }

    @Test
    void startsLocalStandbyWithoutKvcmTopology() {
        FlexlbConfig config = new FlexlbConfig();
        config.setCacheMatching(new LocalStandbyCacheMatchingConfig());
        CacheMatchConfiguration configuration = new CacheMatchConfiguration(new ModelMetaConfig(route(false)), config);
        assertEquals(CacheMatchMode.LOCAL_STANDBY, configuration.getConfiguredMode());
        assertTrue(configuration.isLocalStandbyEnabled());
        assertFalse(configuration.isKvcmEnabled());
        assertFalse(configuration.isLocalSyncEnabled());
    }

    @Test
    void publishesKvcmRuntimeConfigUpdatesToReaders() {
        ConfigService configService = mock(ConfigService.class);
        FlexlbConfig initial = kvcmConfig(false);
        when(configService.loadBalanceConfig()).thenReturn(initial);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<FlexlbConfig>> listener =
                ArgumentCaptor.forClass(Consumer.class);

        CacheMatchConfiguration configuration =
                new CacheMatchConfiguration(new ModelMetaConfig(route(true)), configService);
        assertSame(initial.getKvcmAndStandbyConfig(), configuration.getKvcmClientSettings());
        verify(configService).addUpdateListener(listener.capture());

        FlexlbConfig updated = kvcmConfig(true);
        updated.getKvcmAndStandbyConfig().setTopKHostCount(7);
        updated.getKvcmAndStandbyConfig()
                .setBackendTypes(List.of(KvcmCacheMatchingConfig.BackendType.ST_EVENT_REPORT_L2));
        updated.getKvcmAndStandbyConfig().setMedium(List.of("kvs"));
        listener.getValue().accept(updated);

        assertNotSame(initial.getKvcmAndStandbyConfig(), configuration.getKvcmClientSettings());
        assertSame(updated.getKvcmAndStandbyConfig(), configuration.getKvcmClientSettings());
        assertEquals(7, configuration.getKvcmClientSettings().getTopKHostCount());
        assertEquals(List.of(KvcmCacheMatchingConfig.BackendType.ST_EVENT_REPORT_L2),
                configuration.getKvcmClientSettings().getBackendTypes());
        assertEquals(List.of("kvs"), configuration.getKvcmClientSettings().getMedium());
        assertFalse(configuration.isAutoSwitchEnabled(), "standby keeps the autoSwitch value from startup");
        assertSame(initial.getKvcmAndStandbyConfig().getLocalStandby(), configuration.getLocalStandbyConfig());
    }

    @Test
    void keepsKvcmRuntimeConfigWhenAnUpdateActivatesAnotherMode() {
        ConfigService configService = mock(ConfigService.class);
        FlexlbConfig initial = kvcmConfig(false);
        when(configService.loadBalanceConfig()).thenReturn(initial);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<FlexlbConfig>> listener =
                ArgumentCaptor.forClass(Consumer.class);

        CacheMatchConfiguration configuration =
                new CacheMatchConfiguration(new ModelMetaConfig(route(true)), configService);
        verify(configService).addUpdateListener(listener.capture());

        FlexlbConfig localSync = new FlexlbConfig();
        localSync.setCacheMatching(new LocalSyncCacheMatchingConfig());
        listener.getValue().accept(localSync);

        assertSame(initial.getKvcmAndStandbyConfig(), configuration.getKvcmClientSettings());
        assertEquals(CacheMatchMode.KVCM, configuration.getConfiguredMode());
    }

    private FlexlbConfig kvcmConfig(boolean autoSwitch) {
        KvcmCacheMatchingConfig kvcm = new KvcmCacheMatchingConfig();
        kvcm.getLocalStandby().setAutoSwitch(autoSwitch);
        FlexlbConfig config = new FlexlbConfig();
        config.setCacheMatching(kvcm);
        return config;
    }

    private ServiceRoute route(boolean hasKvcmTopology) {
        ServiceRoute route = new ServiceRoute();
        if (hasKvcmTopology) {
            route.setKvcm(new KvcmConfig());
        }
        return route;
    }
}
