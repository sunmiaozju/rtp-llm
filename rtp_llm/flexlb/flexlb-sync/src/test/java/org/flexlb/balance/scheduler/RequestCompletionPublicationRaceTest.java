package org.flexlb.balance.scheduler;

import org.flexlb.balance.scheduler.RequestSlot.AdmissionHandle;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void deliverySelectedBeforeExpiryKeepsItsResponseEvenBeforeTheFutureIsCompleted() {
        Fixture fixture = fixture();
        Response success = new Response();
        success.setSuccess(true);
        RequestCompletionPublisher.SelectedPublication publishSuccess = fixture.slot().selectPublication(fixture.delivery().publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, success, null, false);
        assertFalse(fixture.slot().future().isDone());
        CompletableFuture<Void> callback = fixture.slot().future().thenAccept(response ->
                assertFalse(Thread.holdsLock(fixture.slot())));
        synchronized (fixture.slot()) {
            RequestLifecycleTestSupport.recordCancellation(fixture.slot(), CancelReason.DEADLINE_EXCEEDED, "request inactive");
            TerminalAction terminal = org.springframework.test.util.ReflectionTestUtils.<TerminalAction>invokeMethod(fixture.slot(), "claimTerminalActionLocked", null,
                    TerminalOutcome.timeout("request inactive"), new Response(), true);
            assertNotNull(terminal);
            assertNull(terminal.publication(), "an already selected delivery owns the frontend result");
            assertEquals(RequestState.Phase.TIMED_OUT,
                    fixture.slot().commitTerminalRecord(terminal).terminal().state());
        }
        assertTrue(publishSuccess.complete());
        assertSame(success, fixture.slot().future().join());
        callback.join();
    }

    @ParameterizedTest
    @EnumSource(TerminalForm.class)
    void terminalSelectionInvalidatesAnUnpublishedAcknowledgementForEveryCompletionKind(TerminalForm form) {
        Fixture fixture = fixture();
        Response failure = new Response();
        failure.setSuccess(false);
        TerminalAction terminal;
        synchronized (fixture.slot()) {
            // ACKNOWLEDGED records the Engine fact, not a selected frontend result.
            terminal = org.springframework.test.util.ReflectionTestUtils.<TerminalAction>invokeMethod(fixture.slot(), "claimTerminalActionLocked", null,
                    TerminalOutcome.fail("worker failed before response publication"), failure, failure != null);
            assertNotNull(terminal.publication());
            fixture.slot().commitTerminalRecord(terminal);
        }
        Response success = new Response();
        success.setSuccess(true);
        assertFalse(fixture.slot().selectPublication(fixture.delivery().publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, success, null, false).complete());
        assertFalse(fixture.slot().future().isDone());
        CompletableFuture<Void> callback = fixture.slot().future().handle((response, error) -> {
            assertFalse(Thread.holdsLock(fixture.slot()));
            return null;
        });
        RequestCompletionPublisher.SelectedPublication publication = switch (form) {
            case RESPONSE -> fixture.slot().selectPublication(terminal.publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, failure, null, false);
            case FAILURE -> fixture.slot().selectPublication(terminal.publication(), RequestCompletionPublisher.ResponseCompletion.FAILURE, null, new IllegalStateException("worker failed"), false);
            case CANCELLATION -> fixture.slot().selectPublication(terminal.publication(), RequestCompletionPublisher.ResponseCompletion.CANCELLATION, null, null, false);
        };
        assertTrue(publication.complete());
        callback.join();
        assertTrue(fixture.slot().future().isDone());
        if (form == TerminalForm.RESPONSE) {
            assertSame(failure, fixture.slot().future().join());
        } else {
            assertTrue(fixture.slot().future().isCompletedExceptionally());
            assertEquals(form == TerminalForm.CANCELLATION, fixture.slot().future().isCancelled());
        }
    }

    @ParameterizedTest
    @EnumSource(TerminalForm.class)
    void externalFutureOperationUnderSlotLockLeavesRequestUnchanged(TerminalForm form) {
        RequestSlot slot = new RequestSlot(mock(RequestCompletionPublisher.class),
                RequestLifecycleTestSupport.context(SchedulingTestConfig.routeConfig(), 702L),
                mock(ExpirationTimer.class), mock(RequestTerminalCleanup.class), () -> {}, null);
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

    private static Fixture fixture() {
        RequestCompletionPublisher publisher = mock(RequestCompletionPublisher.class);
        var config = SchedulingTestConfig.routeConfig();
        BalanceContext context = RequestLifecycleTestSupport.context(config, 701L);
        RequestSlot slot = new RequestSlot(publisher, context, mock(ExpirationTimer.class),
                mock(RequestTerminalCleanup.class), () -> {}, null);
        when(publisher.tryReservePublication(eq(slot), any())).thenAnswer(invocation ->
                new RequestCompletionPublisher.PublicationPermit(publisher, slot, invocation.getArgument(1)));
        ScheduledRequest item = new ScheduledRequest(context, slot.future(), new Response(), null, null,
                null, null, null, slot.createdAtMs());
        RequestSlot.DeliveryPublication delivery;
        synchronized (slot) {
            AdmissionHandle admission = slot.tryBeginAdmissionHandle();
            assertNotNull(admission);
            assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(slot, "tryBindItemForPublicationLocked", item));
            admission.close();
            RequestLifecycleTestSupport.startRouteDelivery(slot);
            delivery = RequestLifecycleTestSupport.acknowledge(slot, 0L).delivery();
            assertNotNull(delivery);
        }
        return new Fixture(slot, delivery);
    }

    private enum TerminalForm { RESPONSE, FAILURE, CANCELLATION }

    private record Fixture(RequestSlot slot, RequestSlot.DeliveryPublication delivery) { }
}
