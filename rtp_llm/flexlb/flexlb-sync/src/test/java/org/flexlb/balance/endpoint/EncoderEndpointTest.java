package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class EncoderEndpointTest {

    @Test
    void closingSeveralPendingRequestsNotifiesCapacityOnce() {
        EndpointEventHandler events = mock(EndpointEventHandler.class);
        EncoderEndpoint endpoint = endpoint(events);
        assertTrue(endpoint.trackSelectedRequest("request-1", 10));
        assertTrue(endpoint.trackSelectedRequest("request-2", 20));
        assertFalse(endpoint.trackSelectedRequest("request-1", 10));

        endpoint.close();
        endpoint.awaitRetirement();

        assertEquals(0, endpoint.pendingEncoderRequestCount());
        assertTrue(endpoint.trackSelectedRequest("request-3", 30));
        assertEquals(0, endpoint.pendingEncoderRequestCount());
        verify(events).onEncoderCapacityChanged();
    }

    private static EncoderEndpoint endpoint(EndpointEventHandler events) {
        return new EncoderEndpoint(WorkerStatus.createDiscovered(
                RoleType.ENCODER, "default", "127.0.0.1", 8001, 8002, ""), events);
    }
}
