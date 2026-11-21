package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.EncoderStrategy;
import org.flexlb.config.ConfigService;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test-only composition root for API integration fixtures.
 *
 * <p>The production scheduler deliberately exposes only its facade. This
 * fixture lives in the scheduler package so tests can assemble the same
 * package-private lifecycle owners without reopening production constructors.
 */
public final class RequestSchedulerTestRuntime implements AutoCloseable {

    private final RequestRegistry lifecycle;
    private final PlacementAvailability placementAvailability =
            new PlacementAvailability();
    private final QueueCapacity queueCapacity = new QueueCapacity();
    private final EndpointRegistry registry;
    private final ConcurrentLinkedQueue<WorkerEndpoint> publishedEndpoints = new ConcurrentLinkedQueue<>();
    private final RequestScheduler scheduler;
    private final SchedulerRuntime runtime;

    public RequestSchedulerTestRuntime(ConfigService configService,
                                       BatchSchedulerReporter batchReporter,
                                       RequestSchedulerReporter requestReporter) {
        this.lifecycle = new RequestRegistry(configService, batchReporter, requestReporter);
        var queues = new PrefillQueueRegistry();
        var reservations = new org.flexlb.balance.schedulev2.decode.DecodeReservations();
        var events = new EndpointEventHandler(lifecycle, queues, configService, reservations, requestReporter);
        this.registry = new EndpointRegistry(configService, events, batchReporter, placementAvailability, queues,
                requestReporter);
        var workers = new org.flexlb.sync.status.WorkerDirectory(registry);
        var model = org.mockito.Mockito.mock(ModelMetaConfig.class);
        org.mockito.Mockito.when(model.requiredRoles()).thenReturn(List.of(RoleType.PREFILL, RoleType.DECODE));
        var router = new DefaultRouter(new EncoderStrategy(workers), configService, model);
        var cache = org.mockito.Mockito.mock(org.flexlb.cache.match.CacheAwareService.class);
        org.mockito.Mockito.when(cache.findMatchingEngines(org.mockito.ArgumentMatchers.any())).thenReturn(
                org.flexlb.cache.domain.CacheMatchResult.empty(org.flexlb.cache.domain.CacheMatchSource.LOCAL_SYNC));
        var health = org.mockito.Mockito.mock(org.flexlb.service.monitor.EngineHealthReporter.class);
        var prefill = new CostBasedPrefillStrategy(workers, cache, health);
        var v2 = new org.flexlb.balance.schedulev2.RequestSchedulerV2(configService, queueCapacity, registry,
                placementAvailability, router, prefill, new DecodeSelector(workers), queues, model,
                new org.flexlb.balance.eviction.DecodePreemptionCoordinator(requestReporter), workers, reservations,
                org.mockito.Mockito.mock(org.flexlb.service.monitor.V2QueueReporter.class));
        this.scheduler = new RequestScheduler(configService, router, lifecycle, v2);
        this.runtime = new SchedulerRuntime(lifecycle, registry, batchReporter, requestReporter, scheduler,
                configService, queueCapacity, model);
    }

    public RequestScheduler scheduler() {
        return scheduler;
    }

    public EndpointRegistry endpointRegistry() {
        return registry;
    }

    public WorkerEndpoint publishEndpoint(WorkerStatus status, WorkerStatus.PreparedStatus prepared) {
        WorkerEndpoint endpoint = registry.publishPreparedEndpoint(status.getLogicalIpPort(), status, prepared).endpoint();
        publishedEndpoints.add(endpoint);
        return endpoint;
    }

    public PlacementAvailability placementAvailability() {
        return placementAvailability;
    }

    public void maintainExpiration() {
        runtime.maintainExpiration();
    }

    /**
     * 将 protobuf 响应转为状态观测，并执行状态更新与通知。
     */
    public void applyStatus(WorkerStatus status, WorkerStatusPB.Builder response) {
        applyStatus(status, EngineStatusConverter.convertToStatusObservation(status, response.build()));
    }

    /** Apply an immutable protocol observation without a mutable DTO hop. */
    public void applyStatus(
            WorkerStatus status,
            WorkerStatus.StatusObservation observation) {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(observation, "observation");
        RoleType role = observation.role();
        WorkerEndpoint endpoint = registry.get(
                role, status.getLogicalIpPort(), status);
        if (endpoint == null) {
            throw new IllegalStateException(
                    "status generation has no published endpoint: "
                            + status.getLogicalIpPort() + "#" + status.getGenerationId());
        }

        Runnable statusNotifier;
        status.lock.lock();
        try {
            long responseVersion = Objects.requireNonNull(
                    observation.statusVersion(), "response status version");
            long committedVersion = status.appliedStatusCursor().statusVersion();
            if (responseVersion < committedVersion) {
                throw new IllegalArgumentException(
                        "worker status version regressed: committed="
                                + committedVersion + ", response=" + responseVersion);
            }
            if (responseVersion == committedVersion) {
                statusNotifier = endpoint.observeStatusHeartbeat(
                        status, observation);
            } else {
                WorkerStatus.PreparedStatus prepared =
                        status.prepareNewStatus(observation);
                statusNotifier = endpoint.applyPreparedStatus(status, prepared);
            }
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
                EndpointRegistry.getEndpointsByRole(endpoint.getStatus().getRole()).remove(endpoint.ipPort(), endpoint);
            }
            publishedEndpoints.clear();
        }
    }

}
