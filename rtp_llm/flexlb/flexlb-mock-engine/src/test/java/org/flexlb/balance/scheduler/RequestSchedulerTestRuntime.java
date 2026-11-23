package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.EncoderStrategy;
import org.flexlb.balance.strategy.EndpointRoundRobin;
import org.flexlb.config.ConfigService;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Test-only composition root for mock-engine end-to-end scheduler tests. */
public final class RequestSchedulerTestRuntime implements AutoCloseable {

    private final RequestRegistry lifecycle;
    private final PlacementAvailability placementAvailability =
            new PlacementAvailability();
    private final QueueCapacity queueCapacity = new QueueCapacity();
    private final EndpointRegistry registry;
    private final List<WorkerEndpoint> publishedEndpoints = new ArrayList<>();
    private final RequestScheduler scheduler;
    private final SchedulerRuntime runtime;

    public RequestSchedulerTestRuntime(ConfigService configService,
                                       BatchSchedulerReporter batchReporter,
                                       RequestSchedulerReporter requestReporter,
                                       java.util.function.Function<BalanceContext, Integer> selectedPrefillPort) {
        this.lifecycle = new RequestRegistry(configService, batchReporter, requestReporter);
        var queues = new PrefillQueueRegistry();
        var events = new EndpointEventHandler(lifecycle, queues, configService, requestReporter);
        this.registry = new EndpointRegistry(configService, events, batchReporter, placementAvailability, queues,
                requestReporter);
        var workers = new org.flexlb.sync.status.WorkerDirectory(registry);
        var model = org.mockito.Mockito.mock(ModelMetaConfig.class);
        org.mockito.Mockito.when(model.getRequiredRoles()).thenReturn(Set.of(RoleType.PREFILL, RoleType.DECODE));
        var router = new DefaultRouter(new EncoderStrategy(workers), configService, model, null);
        var cache = org.mockito.Mockito.mock(org.flexlb.cache.match.CacheAwareService.class);
        org.mockito.Mockito.when(cache.findMatchingEngines(org.mockito.ArgumentMatchers.any())).thenReturn(
                org.flexlb.cache.domain.CacheMatchResult.empty(org.flexlb.cache.domain.CacheMatchSource.LOCAL_SYNC));
        var health = org.mockito.Mockito.mock(org.flexlb.service.monitor.EngineHealthReporter.class);
        var rotation = new EndpointRoundRobin();
        var prefill = new CostBasedPrefillStrategy(workers, cache, health, rotation) {
            @Override
            public org.flexlb.balance.strategy.RequestWorkLoadEstimateResult prepare(BalanceContext context,
                                                                                           RoleType role, String group) {
                var prepared = super.prepare(context, role, group);
                if (prepared == null) {
                    return null;
                }
                Integer port = selectedPrefillPort.apply(context);
                if (port == null) {
                    return prepared;
                }
                var matching = prepared.candidates().stream()
                        .filter(candidate -> candidate.endpoint().getHttpPort() == port).toList();
                return matching.isEmpty() ? null
                        : org.flexlb.balance.strategy.RequestWorkLoadEstimateResult.success(prepared.cacheMatch(), matching);
            }
        };
        var v2 = new org.flexlb.balance.schedulev2.RequestSchedulerV2(configService, queueCapacity, registry,
                placementAvailability, router, prefill, new DecodeSelector(workers), queues, model,
                new org.flexlb.balance.eviction.DecodePreemptionCoordinator(requestReporter),
                org.mockito.Mockito.mock(org.flexlb.service.monitor.V2QueueReporter.class));
        this.scheduler = new RequestScheduler(configService, router, lifecycle, v2);
        this.runtime = new SchedulerRuntime(lifecycle, registry, batchReporter, requestReporter, scheduler,
                configService, queueCapacity, model);
    }

    public RequestScheduler scheduler() {
        return scheduler;
    }

    public void publishEndpoint(WorkerStatus status, WorkerStatus.PreparedStatus prepared) {
        WorkerEndpoint endpoint = registry.publishPreparedEndpoint(status.getLogicalIpPort(), status, prepared).endpoint();
        status.recordSuccessfulPoll(prepared.observation().alive());
        publishedEndpoints.add(endpoint);
    }

    public EndpointRegistry endpointRegistry() {
        return registry;
    }

    public PlacementAvailability placementAvailability() {
        return placementAvailability;
    }

    /**
     * 发布新版本状态，或用同版本心跳刷新存活状态。
     */
    public void applyStatus(WorkerStatus status, WorkerStatusPB response) {
        WorkerStatus.StatusObservation observation = EngineStatusConverter.convertToStatusObservation(status, response);
        RoleType role = observation.role();
        WorkerEndpoint endpoint = registry.get(
                role, status.getLogicalIpPort(), status);
        if (endpoint == null) {
            throw new IllegalStateException(
                    "status generation has no published endpoint: "
                            + status.getIpPort() + "#" + status.getGenerationId());
        }

        Runnable statusNotifier;
        status.lock.lock();
        try {
            long responseVersion = response.getStatusVersion();
            long committedVersion = status.appliedStatusCursor().statusVersion();
            if (responseVersion < committedVersion) {
                throw new IllegalArgumentException(
                        "worker status version regressed: committed="
                                + committedVersion + ", response=" + responseVersion);
            }

            if (responseVersion == committedVersion) {
                statusNotifier = endpoint.observeStatusHeartbeat(status, observation);
            } else {
                WorkerStatus.PreparedStatus prepared =
                        status.prepareNewStatus(observation);
                statusNotifier = endpoint.applyPreparedStatus(status, prepared);
            }
            status.recordSuccessfulPoll(observation.alive());
        } finally {
            status.lock.unlock();
        }
        statusNotifier.run();
    }

    @Override
    public void close() {
        try {
            runtime.shutdown();
        } finally {
            // Test JVMs reuse the static address maps across scheduler instances.
            for (WorkerEndpoint endpoint : publishedEndpoints) {
                WorkerStatus status = endpoint.getStatus();
                EndpointRegistry.getEndpointsByRole(status.getRole()).remove(status.getLogicalIpPort(), endpoint);
            }
            publishedEndpoints.clear();
        }
    }

}
