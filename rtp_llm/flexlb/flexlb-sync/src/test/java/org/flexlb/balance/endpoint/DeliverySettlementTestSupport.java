package org.flexlb.balance.endpoint;

import org.flexlb.balance.endpoint.prefill.PrefillActiveIndex;
import org.flexlb.balance.endpoint.prefill.PrefillState;
import org.flexlb.balance.endpoint.prefill.ReservationResult;
import org.flexlb.balance.endpoint.prefill.WorkerStatusFact;
import org.flexlb.balance.endpoint.prefill.batch.BatchReservation;
import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.grpc.WorkerStatusPBs;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real endpoint accounting used by delivery callback ordering tests. */
public final class DeliverySettlementTestSupport {
    private final ReentrantLock lock = new ReentrantLock();
    public final PrefillState prefill = new PrefillState(lock,
                PrefillActiveIndex.ordered(4, Comparator.comparing(ScheduledRequest::requestId)),
            System::currentTimeMillis, () -> { });
    private final EndpointGenerationLifecycle generation = new EndpointGenerationLifecycle(() -> { });

    public void enqueue(ScheduledRequest item) {
        lock.lock();
        try {
            assertTrue(prefill.enqueueActive(item, Long.MAX_VALUE));
        } finally {
            lock.unlock();
        }
    }

    public ReservationResult<BatchReservation> reserveBatch(ScheduledRequest head, long batchId, int maxBatches) {
        return prefill.reserveBatch(head, batchId, maxBatches, generation.tryAcquireHandoff());
    }

    public void commit(long batchId, List<ScheduledRequest> items) {
        lock.lock();
        try {
            for (ScheduledRequest item : items) {
                assertTrue(prefill.enqueueActive(item, Long.MAX_VALUE));
            }
        } finally {
            lock.unlock();
        }
        try (var reservation = prefill.reserveBatch(items.getFirst(), batchId, 2,
                generation.tryAcquireHandoff()).reservation()) {
            assertNotNull(reservation);
            try (var handoff = reservation.commit(items, 100L)) {
                assertNotNull(handoff);
            }
        }
    }

    public List<WorkerStatusFact> finish(long batchId, ScheduledRequest item) {
        var response = WorkerStatusPBs.status(RoleType.PREFILL)
                .addFinishedTaskList(WorkerStatusPBs.task(item.requestId()).setBatchId(batchId));
        var observation = EngineStatusConverter.convertToStatusObservation(
                EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090), response.build());
        var result = prefill.updateFromWorkerStatus(observation, ignored -> 100L, () -> {}, () -> {});
        assertNull(result.publicationFailure());
        return result.schedulerFacts();
    }

    public static void queueDecode(DecodeEndpoint endpoint, DecodeEndpoint.ReservationHandle reservation) {
        var response = WorkerStatusPB.newBuilder().setAvailableKvCache(10_000L).setTotalKvCache(10_000L);
        EndpointTestSupport.applyStatus(endpoint, response).run();
        try (var hold = endpoint.tryHold()) {
            assertTrue(endpoint.markQueued(hold, reservation));
        }
    }

    public static void dispatchDecode(DecodeEndpoint endpoint, DecodeEndpoint.ReservationHandle reservation) {
        queueDecode(endpoint, reservation);
        var permit = endpoint.acquireDispatchPermit(reservation, new DecodeEndpoint.CapacityLimit(0, 100L)).permit();
        assertNotNull(permit);
        assertEquals(DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED,
                permit.dispatch());
    }

    public static void decodeStatus(DecodeEndpoint endpoint, long requestId, boolean finished) {
        var task = WorkerStatusPBs.task(Long.toString(requestId), finished ? null : TaskPhase.KV_ALLOCATED);
        var response = WorkerStatusPB.newBuilder().setAvailableKvCache(10_000L).setTotalKvCache(10_000L);
        if (finished) {
            response.addFinishedTaskList(task);
        } else {
            response.addRunningTaskInfo(task);
        }
        EndpointTestSupport.applyStatus(endpoint, response).run();
    }
}
