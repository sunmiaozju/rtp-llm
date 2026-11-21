package org.flexlb.balance.scheduler;

import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Frontend result selection precedes unlocked publication and cannot undo TTL cleanup. */
class RequestCompletionPublicationRaceTest {

    @Test
    void selectedEncoderRouteWinsAgainstLaterCancellationBeforeFutureCompletes() {
        var publisher = mock(RequestCompletionPublisher.class);
        var slot = new RequestSlot(publisher, context("701"), mock(ExpirationTimer.class),
                mock(RequestTerminalCleanup.class));
        when(publisher.tryReservePublication(eq(slot), any())).thenAnswer(call ->
                new RequestCompletionPublisher.PublicationPermit(publisher, slot, call.getArgument(1)));
        var endpoint = mock(org.flexlb.balance.endpoint.EncoderEndpoint.class);
        when(endpoint.trackSelectedRequest("701", 10L)).thenReturn(true);
        assertTrue(slot.claimEncoderRoute(endpoint));
        var success = new Response();
        success.setSuccess(true);
        assertTrue(slot.publishEncoderRoute(success));
        assertFalse(slot.future().isDone());

        slot.cancelRequest(0L, CancelReason.CLIENT_CANCELLED);
        var result = org.mockito.ArgumentCaptor.forClass(RequestCompletionPublisher.SelectedPublication.class);
        org.mockito.Mockito.verify(publisher).submit(result.capture());
        assertTrue(result.getValue().complete());
        assertSame(success, slot.future().join());
    }

    @ParameterizedTest
    @EnumSource(TerminalForm.class)
    void externalFutureOperationUnderSlotLockLeavesRequestUnchanged(TerminalForm form) {
        RequestSlot slot = new RequestSlot(mock(RequestCompletionPublisher.class),
                context("702"),
                mock(ExpirationTimer.class), mock(RequestTerminalCleanup.class));
        synchronized (slot) {
            assertThrows(IllegalStateException.class, () -> {
                switch (form) {
                    case RESPONSE -> slot.future().complete(new Response());
                    case FAILURE -> slot.future().completeExceptionally(new IllegalStateException("failure"));
                    case CANCELLATION -> slot.future().cancel(false);
                }
            });
            assertTrue(slot.isOpen());
            assertEquals(RequestState.Phase.QUEUED, slot.snapshot().state());
            assertFalse(slot.future().isDone());
        }
    }

    private static BalanceContext context(String id) {
        return org.flexlb.balance.schedulev2.V2TestSupport.request(SchedulingTestConfig.newConfig(), id, 50, 10).ctx();
    }

    private enum TerminalForm { RESPONSE, FAILURE, CANCELLATION }

}
