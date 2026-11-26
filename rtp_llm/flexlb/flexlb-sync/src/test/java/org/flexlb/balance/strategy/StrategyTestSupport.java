package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.scheduler.RequestRegistry;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.mockito.Mockito;

import java.util.Map;

/** Package-local fixtures built only from the frozen endpoint-facing ports. */
final class StrategyTestSupport {

    private StrategyTestSupport() {
    }

    static FlexlbConfig config() {
        FlexlbConfig config = new FlexlbConfig();
        config.getRequestLifecycle().getRequest().setTimeoutMs(60_000L);
        config.getRequestLifecycle().getDecision().setLifetime(2.0);
        return config;
    }

    static EndpointRegistry endpointRegistry(ConfigService configService) {
        var queues = new PrefillQueueRegistry();
        var reporter = Mockito.mock(RequestSchedulerReporter.class);
        var events = new EndpointEventHandler(Mockito.mock(RequestRegistry.class), queues, configService, reporter);
        return new EndpointRegistry(configService, events, Mockito.mock(BatchSchedulerReporter.class),
                new PlacementAvailability(), queues, reporter);
    }

    static WorkerStatus workerStatus(RoleType role,
                                     String group,
                                     String ip,
                                     int httpPort,
                                     int grpcPort,
                                     boolean alive,
                                     long availableKv,
                                     long totalKv) {
        WorkerStatus status =
                WorkerStatus.createDiscovered(role, group, ip, httpPort, grpcPort, "test-site", null, 0, 1, false);
        WorkerStatusPB.Builder response = response(role, alive, availableKv, totalKv,
                Math.max(1L, status.appliedStatusCursor().statusVersion() + 1L));
        publish(status, response);
        return status;
    }

    static WorkerStatusPB.Builder response(RoleType role,
                                           boolean alive,
                                           long availableKv,
                                           long totalKv,
                                           long statusVersion) {
        return WorkerStatusPBs.status(role)
                .setAlive(alive)
                .setAvailableKvCache(availableKv)
                .setTotalKvCache(totalKv)
                .setStatusVersion(statusVersion);
    }

    static void publish(WorkerStatus status, WorkerStatusPB.Builder response) {
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build()));
            status.publishPreparedStatus(prepared);
            status.recordSuccessfulPoll(response.getAlive());
        } finally {
            status.lock.unlock();
        }
    }

    static WorkerEndpoint publishEndpoint(EndpointRegistry registry,
                                          RoleType role,
                                          String address,
                                          WorkerStatus source) {
        WorkerStatus status = WorkerStatus.createDiscovered(role, source.getGroup(), source.getIp(), source.getPort(),
                source.getGrpcPort(), source.getSite(), source.getDeploymentName(), source.getEngineIndex(),
                source.getMultiEngineNum(), false);
        WorkerStatus.StatusObservation observation =
                status.bindStatusObservation(
                        source.committedEngineObservation(),
                        true,
                        1L,
                        0L,
                        Map.of());
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(observation);
            WorkerEndpoint endpoint = registry.publishPreparedEndpoint(
                    address, status, prepared).endpoint();
            status.recordSuccessfulPoll(observation.alive());
            return endpoint;
        } finally {
            status.lock.unlock();
        }
    }

    static void setCacheStatus(WorkerStatus status, long blockSize, long availableKv) {
        WorkerStatusPB.Builder response = response(status.getRole(), status.pollHealth().reportedAlive(),
                status.getAvailableKvCacheTokens(), status.getTotalKvCacheTokens(),
                Math.max(1L, status.appliedStatusCursor().statusVersion() + 1L));
        response.setBlockSize(blockSize);
        publish(status, response);
    }

}
