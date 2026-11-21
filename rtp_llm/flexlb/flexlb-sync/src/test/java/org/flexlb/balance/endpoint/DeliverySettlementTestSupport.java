package org.flexlb.balance.endpoint;

import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.grpc.WorkerStatusPBs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real endpoint accounting used by delivery callback ordering tests. */
public final class DeliverySettlementTestSupport {
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
