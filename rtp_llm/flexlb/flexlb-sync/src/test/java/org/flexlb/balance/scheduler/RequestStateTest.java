package org.flexlb.balance.scheduler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Contracts of the immutable request view exposed by RequestRegistry. */
class RequestStateTest {

    @Test
    void requestScopedDeliveryCannotCarryBatchIdentity() {
        assertThrows(IllegalArgumentException.class, () -> state(
                RequestState.Phase.ACKNOWLEDGED,
                DeliveryClaimKind.ROUTE_DECISION,
                17L));
    }

    @Test
    void onlyTerminalPhasesAreTerminal() {
        assertFalse(RequestState.Phase.QUEUED.isTerminal());
        assertFalse(RequestState.Phase.CANCEL_REQUESTED.isTerminal());
        assertTrue(RequestState.Phase.CANCELLED.isTerminal());
        assertTrue(RequestState.Phase.TIMED_OUT.isTerminal());
        assertTrue(RequestState.Phase.FAILED.isTerminal());
        assertTrue(RequestState.Phase.COMPLETED.isTerminal());
    }

    private static RequestState state(RequestState.Phase phase,
                                      DeliveryClaimKind claim,
                                      long batchId) {
        return new RequestState(
                "1", 0L, phase, claim, batchId,
                10L, 11L, "test");
    }
}
