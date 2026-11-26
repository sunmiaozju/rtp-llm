package org.flexlb.sync.runner;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.mockito.Mockito;

/** Package-local fixtures for the frozen status/endpoint composition boundary. */
public final class RunnerTestSupport {

    private static final EndpointEventHandler NOOP_EVENT_SINK =
            org.flexlb.balance.endpoint.EndpointTestSupport.noopEventSink();

    private RunnerTestSupport() {
    }

    public static EndpointEventHandler eventSink() {
        return NOOP_EVENT_SINK;
    }

    public static EndpointRegistry endpointRegistry(ConfigService configService) {
        return new EndpointRegistry(configService, NOOP_EVENT_SINK, Mockito.mock(BatchSchedulerReporter.class),
                new PlacementAvailability(), new PrefillQueueRegistry(),
                org.mockito.Mockito.mock(org.flexlb.service.monitor.RequestSchedulerReporter.class));
    }

    public static WorkerStatus discovered(RoleType role, String group, String ip, int port, int grpcPort, String site) {
        return WorkerStatus.createDiscovered(role, group, ip, port, grpcPort, site, null, 0, 1, false);
    }

    public static WorkerStatus alive(
            RoleType role,
            String group,
            String ip,
            int port,
            int grpcPort,
            String site) {
        WorkerStatus status = discovered(
                role, group, ip, port, grpcPort, site);
        publish(status, response(status, true, 1L));
        return status;
    }

    public static WorkerStatusPB.Builder response(WorkerStatus status, boolean alive, long version) {
        return WorkerStatusPBs.status(status.getRole()).setAlive(alive).setStatusVersion(version);
    }

    public static WorkerStatusPB.Builder response(WorkerStatus status,
                                                  boolean alive,
                                                  long version,
                                                  long availableKv,
                                                  long totalKv,
                                                  double stepLatencyMs) {
        return response(status, alive, version).setAvailableKvCache(availableKv)
                .setTotalKvCache(totalKv)
                .setStepLatencyMs(stepLatencyMs);
    }

    public static void publish(WorkerStatus status, WorkerStatusPB.Builder response) {
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

    public static WorkerEndpoint publishEndpoint(EndpointRegistry registry,
                                                 RoleType role,
                                                 String address,
                                                 WorkerStatus status) {
        return publishEndpoint(registry, address, status, response(status, true, 1L));
    }

    static WorkerEndpoint publishEndpoint(EndpointRegistry registry,
                                          String address,
                                          WorkerStatus status,
                                          WorkerStatusPB.Builder response) {
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build()));
            WorkerEndpoint endpoint = registry
                    .publishPreparedEndpoint(address, status, prepared)
                    .endpoint();
            status.recordSuccessfulPoll(true);
            return endpoint;
        } finally {
            status.lock.unlock();
        }
    }

}
