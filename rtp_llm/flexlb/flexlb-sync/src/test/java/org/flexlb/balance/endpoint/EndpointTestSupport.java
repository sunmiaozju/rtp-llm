package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.RoleTypePB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;

/** Shared fixtures which exercise only the frozen endpoint-facing ports. */
public final class EndpointTestSupport {

    private EndpointTestSupport() {
    }

    public static WorkerStatus.StatusObservation statusObservation(RoleType role) {
        var observation = org.mockito.Mockito.mock(WorkerStatus.StatusObservation.class);
        org.mockito.Mockito.when(observation.role()).thenReturn(role);
        return observation;
    }

    static PrefillEndpoint standalonePrefillEndpoint(WorkerStatus status,
                                                     FlexlbConfig config,
                                                     EndpointEventHandler events,
                                                     org.flexlb.service.monitor.RequestSchedulerReporter reporter) {
        return new PrefillEndpoint(status, () -> config, events,
                new org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry(), reporter);
    }

    public static EndpointEventHandler noopEventSink() {
        var configService = org.mockito.Mockito.mock(org.flexlb.config.ConfigService.class);
        org.mockito.Mockito.when(configService.loadBalanceConfig())
                .thenReturn(org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig());
        var events = org.mockito.Mockito.mock(EndpointEventHandler.class);
        org.mockito.Mockito.when(events.getConfigService()).thenReturn(configService);
        return events;
    }

    public static WorkerStatus workerStatus(RoleType role, String ip, int port, int grpcPort) {
        return WorkerStatus.createDiscovered(role, null, ip, port, grpcPort, null, null, 0, 1, false);
    }

    public static WorkerStatus workerStatus(RoleType role,
                                            String group,
                                            String ip,
                                            int port,
                                            int grpcPort,
                                            String site) {
        return WorkerStatus.createDiscovered(role, group, ip, port, grpcPort, site, null, 0, 1, false);
    }

    static Runnable applyStatus(WorkerEndpoint endpoint, WorkerStatusPB.Builder response) {
        WorkerStatus status = endpoint.getStatus();
        prepareResponse(status, response);
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build()));
            Runnable statusNotifier =
                    endpoint.applyPreparedStatus(status, prepared);
            status.recordSuccessfulPoll(response.getAlive());
            return statusNotifier;
        } finally {
            status.lock.unlock();
        }
    }

    static void publishStatus(WorkerStatus status, WorkerStatusPB.Builder response) {
        prepareResponse(status, response);
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
                                          WorkerStatus status) {
        WorkerStatusPB.Builder response = WorkerStatusPBs.status(role).setAlive(true).setStatusVersion(1L);
        return publishEndpoint(registry, role, address, status, response);
    }

    static WorkerEndpoint publishEndpoint(EndpointRegistry registry,
                                          RoleType role,
                                          String address,
                                          WorkerStatus status,
                                          WorkerStatusPB.Builder response) {
        prepareResponse(status, response);
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build()));
            return registry.publishPreparedEndpoint(
                    address, status, prepared).endpoint();
        } finally {
            status.lock.unlock();
        }
    }

    private static void prepareResponse(WorkerStatus status, WorkerStatusPB.Builder response) {
        // This helper models a successful status poll for an active generation.
        // Retirement tests must exercise the explicit dead-status path instead.
        response.setAlive(true);
        if (response.getRoleType() == RoleTypePB.ROLE_TYPE_PDFUSION && response.getRole().isEmpty()) {
            response.setRoleType(WorkerStatusPBs.role(status.getRole()));
        }
        long committedVersion =
                status.appliedStatusCursor().statusVersion();
        long responseVersion = response.getStatusVersion();
        if (responseVersion <= 0L || responseVersion <= committedVersion) {
            response.setStatusVersion(Math.max(
                    1L, committedVersion + 1L));
        }
    }

}
