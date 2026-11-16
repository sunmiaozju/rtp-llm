package org.flexlb.balance.queue;

import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.estimate.PrefillTtftEstimator;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class WorkerBatcherStatusSnapshotTest {

    @Test
    void configuredFallbackBacksUnpublishedEngineCapacities() {
        FlexlbConfig config = new FlexlbConfig();
        config.setFallbackBatchTokenCapacity(2_097_152L);
        WorkerStatus status = WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group-a", "10.0.0.1",
                8080, 9090, "site-a");
        publish(status, statusResponse(0L, 0L, 0L, 1L));
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        when(endpoint.getStatus()).thenReturn(status);
        WorkerBatcher runtime = new WorkerBatcher(
                "snapshot-test", endpoint, config,
                mock(DeliveryStrategy.class),
                mock(PrefillTtftEstimator.class),
                mock(EndpointEventHandler.class), new QueueCapacity());

        assertEquals(2_097_152L, runtime.captureLoadView()
                .queue().constraints().batchTokenCapacity());
    }

    @Test
    void maxSequenceLengthBacksUnpublishedBatchTokenCapacity() {
        WorkerStatusPB.Builder response = statusResponse(0L, 0L, 0L, 1L);
        response.setMaxSeqLen(150L);
        WorkerStatus status = WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group-a", "10.0.0.1",
                8080, 9090, "site-a");
        publish(status, response);

        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        when(endpoint.getStatus()).thenReturn(status);
        WorkerBatcher runtime = new WorkerBatcher(
                "snapshot-test", endpoint, org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig(),
                mock(DeliveryStrategy.class),
                mock(PrefillTtftEstimator.class),
                mock(EndpointEventHandler.class), new QueueCapacity());

        assertEquals(150L, runtime.captureLoadView()
                .queue().constraints().batchTokenCapacity());
    }

    @Test
    void batchTokenEstimateUsesOneCoherentEngineObservation() {
        WorkerStatusPB.Builder first = statusResponse(700L, 600L, 1_000L, 1L);
        WorkerStatusPB.Builder second = statusResponse(3L, 5L, 10L, 2L);
        WorkerStatus status = spy(WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group-a", "10.0.0.1",
                8080, 9090, "site-a"));
        publish(status, first);

        AtomicBoolean publishedSecond = new AtomicBoolean();
        doAnswer(invocation -> {
            WorkerStatus.EngineObservation captured =
                    (WorkerStatus.EngineObservation)
                            invocation.callRealMethod();
            if (publishedSecond.compareAndSet(false, true)) {
                publish(status, second);
            }
            return captured;
        }).when(status).committedEngineObservation();

        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        when(endpoint.getStatus()).thenReturn(status);
        WorkerBatcher runtime = new WorkerBatcher(
                "snapshot-test",
                endpoint,
                org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig(),
                mock(DeliveryStrategy.class),
                mock(PrefillTtftEstimator.class),
                mock(EndpointEventHandler.class), new QueueCapacity());

        GroupPlanner.Constraints capacity = runtime
                .captureLoadView().queue().constraints();
        assertEquals(700L, capacity.batchTokenCapacity());
        assertEquals(Long.MAX_VALUE, capacity.batchKvCapacity());
        assertTrue(publishedSecond.get());
        assertEquals(5L,
                status.committedEngineObservation()
                        .availableKvCacheTokens());
    }

    private static void publish(WorkerStatus status, WorkerStatusPB.Builder response) {
        status.lock.lock();
        try {
            status.publishPreparedStatus(status
                    .prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build())));
        } finally {
            status.lock.unlock();
        }
    }

    private static WorkerStatusPB.Builder statusResponse(long maxBatchTokens,
                                                         long availableKvTokens,
                                                         long totalKvTokens,
                                                         long statusVersion) {
        WorkerStatusPB.Builder response = WorkerStatusPB.newBuilder();
        response.setRoleType(WorkerStatusPBs.role(RoleType.PREFILL));
        response.setAlive(true);
        response.setMaxBatchTokensSize(maxBatchTokens);
        response.setAvailableKvCache(availableKvTokens);
        response.setTotalKvCache(totalKvTokens);
        response.setStatusVersion(statusVersion);
        return response;
    }
}
