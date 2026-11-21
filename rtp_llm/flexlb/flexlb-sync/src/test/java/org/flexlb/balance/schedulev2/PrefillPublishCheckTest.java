package org.flexlb.balance.schedulev2;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PrefillPublishCheckTest {

    private final EndpointRegistry endpoints = mock(EndpointRegistry.class);
    private final PrefillEndpoint prefill = V2TestSupport.prefillEndpoint("p1:8000");
    private final Request request = V2TestSupport.request(SchedulingTestConfig.newConfig(), "1", 50, 16L);
    private final PrefillPublishCheck check = new PrefillPublishCheck(endpoints, mock(PrefillQueueRegistry.class));

    PrefillPublishCheckTest() {
        V2TestSupport.choose(request, prefill, 0L, 10L);
        when(endpoints.get(RoleType.PREFILL, "p1:8000")).thenReturn(prefill);
    }

    @Test
    void currentPrefillIsAvailableForPublication() {
        assertTrue(check.isAvailable(request));
    }

    @Test
    void retiringPrefillIsRejectedEvenWhileStillInRegistry() {
        when(prefill.isRetired()).thenReturn(true);
        assertFalse(check.isAvailable(request));
    }

    @Test
    void replacedPrefillAtSameAddressIsRejected() {
        PrefillEndpoint replacement = V2TestSupport.prefillEndpoint("p1:8000");
        when(endpoints.get(RoleType.PREFILL, "p1:8000")).thenReturn(replacement);
        assertFalse(check.isAvailable(request));
    }

    @Test
    void removedPrefillIsRejected() {
        when(endpoints.get(RoleType.PREFILL, "p1:8000")).thenReturn(null);
        assertFalse(check.isAvailable(request));
    }
}
