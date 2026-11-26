package org.flexlb.engine.grpc.client;

import org.flexlb.config.CacheMatchConfiguration;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.KvcmCacheMatchingConfig;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusProvider;
import org.flexlb.dao.route.Endpoint;
import org.flexlb.dao.route.GroupRoleEndPoint;
import org.flexlb.dao.route.KvcmConfig;
import org.flexlb.dao.route.RoleType;
import org.flexlb.dao.route.ServiceRoute;
import org.flexlb.enums.KvCacheGroupMode;
import org.flexlb.kvcm.grpc.QueryType;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KvcmWorkerMetadataResolverTest {

    @Test
    void resolvesNamespaceAndQueryTypeFromOneWorkerStatusRefresh() {
        WorkerStatus workerStatus = workerStatus(
                "prefill-deployment", KvCacheGroupMode.WITH_MAMBA);
        KvcmWorkerMetadataResolver resolver = new KvcmWorkerMetadataResolver(
                configuration(null), provider(List.of(workerStatus)));

        resolver.refreshNamespacesAndQueryTypes();

        assertEquals("prefill-deployment_2192",
                resolver.resolveNamespace(RoleType.PREFILL, "default", 2192));
        assertEquals(QueryType.QT_PREFIX_MATCH_WITH_MAMBA,
                resolver.resolveQueryType(RoleType.PREFILL, "default"));
        assertEquals(QueryType.QT_PREFIX_MATCH_WITH_MAMBA,
                resolver.resolveQueryType(RoleType.PREFILL, null));
    }

    @Test
    void selectsSwaQueryForHybridSglangWorkers() {
        WorkerStatus workerStatus = workerStatus("mimo-prefill", KvCacheGroupMode.WITH_SWA);
        KvcmWorkerMetadataResolver resolver =
                new KvcmWorkerMetadataResolver(configuration(null), provider(List.of(workerStatus)));
        resolver.refreshNamespacesAndQueryTypes();
        assertEquals(QueryType.QT_PREFIX_MATCH_WITH_SWA, resolver.resolveQueryType(RoleType.PREFILL, "default"));
        assertEquals(5, resolver.resolveQueryType(RoleType.PREFILL, "default").getNumber());
    }

    @Test
    void configuredNamespaceTakesPriorityWhileQueryTypeStillUsesWorkerStatus() {
        WorkerStatus workerStatus = workerStatus(
                "ignored-deployment", KvCacheGroupMode.FULL_ATTENTION_ONLY);
        KvcmWorkerMetadataResolver resolver = new KvcmWorkerMetadataResolver(
                configuration("configured-namespace"), provider(List.of(workerStatus)));

        resolver.refreshNamespacesAndQueryTypes();

        assertEquals("configured-namespace_1024",
                resolver.resolveNamespace(RoleType.PREFILL, "default", 1024));
        assertEquals(QueryType.QT_PREFIX_MATCH,
                resolver.resolveQueryType(RoleType.PREFILL, "default"));
    }

    @Test
    void unspecifiedModeDoesNotOverwriteLastTrustedQueryType() {
        WorkerStatus workerStatus = workerStatus(
                "prefill-deployment", KvCacheGroupMode.WITH_MAMBA);
        KvcmWorkerMetadataResolver resolver = new KvcmWorkerMetadataResolver(
                configuration(null), provider(List.of(workerStatus)));
        resolver.refreshNamespacesAndQueryTypes();

        publishMode(workerStatus, KvCacheGroupMode.UNSPECIFIED, 2L);
        resolver.refreshNamespacesAndQueryTypes();

        assertEquals(QueryType.QT_PREFIX_MATCH_WITH_MAMBA,
                resolver.resolveQueryType(RoleType.PREFILL, "default"));
    }

    private WorkerStatus workerStatus(String deploymentName, KvCacheGroupMode mode) {
        WorkerStatus workerStatus = WorkerStatus.createDiscovered(RoleType.PREFILL, "default", "127.0.0.1", 8080, 8081,
                "test", deploymentName, 0, 1, false);
        publishMode(workerStatus, mode, 1L);
        return workerStatus;
    }

    private void publishMode(WorkerStatus workerStatus, KvCacheGroupMode mode, long statusVersion) {
        WorkerStatus.EngineObservation engine = new WorkerStatus.EngineObservation(RoleType.PREFILL, 0L, 0L, 0L,
                Map.of(), 0.0, 0L, 0L, 0L, 0L, null, 0L, 0, 0, mode, 0L, 0L, 0L, 0L, null);

        workerStatus.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared = workerStatus
                    .prepareNewStatus(workerStatus.bindStatusObservation(engine, true, statusVersion, 0L, Map.of()));
            workerStatus.publishPreparedStatus(prepared);
            workerStatus.recordSuccessfulPoll(true);
        } finally {
            workerStatus.lock.unlock();
        }
    }

    private WorkerStatusProvider provider(Collection<WorkerStatus> workerStatuses) {
        return new WorkerStatusProvider() {
            @Override
            public Collection<WorkerStatus> getWorkerStatuses(RoleType roleType, String group) {
                return workerStatuses;
            }
        };
    }

    private ModelMetaConfig modelMetaConfig(String namespace) {
        Endpoint endpoint = new Endpoint();
        endpoint.setAddress("v-prefill");

        GroupRoleEndPoint group = new GroupRoleEndPoint();
        group.setGroup("default");
        group.setPrefillEndpoint(endpoint);

        KvcmConfig kvcm = new KvcmConfig();
        kvcm.setNamespace(namespace);

        ServiceRoute route = new ServiceRoute();
        route.setKvcm(kvcm);
        route.setRoleEndpoints(List.of(group));

        return new ModelMetaConfig(route);
    }

    private CacheMatchConfiguration configuration(String namespace) {
        FlexlbConfig flexlbConfig = new FlexlbConfig();
        flexlbConfig.setCacheMatching(new KvcmCacheMatchingConfig());
        return new CacheMatchConfiguration(modelMetaConfig(namespace), flexlbConfig);
    }
}
