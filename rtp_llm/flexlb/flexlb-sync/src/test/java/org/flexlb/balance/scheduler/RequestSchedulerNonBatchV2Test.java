package org.flexlb.balance.scheduler;

import org.flexlb.balance.schedulev2.RequestSchedulerV2;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.route.RequestPhase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RequestSchedulerNonBatchV2Test {

    @Test
    void generationRequestsGoToSchedulerV2() {
        FlexlbConfig config = SchedulingTestConfig.routeConfig();
        config.getDispatcher().setType(DispatcherConfig.Type.NON_BATCH_V2);
        RequestRegistry registry = mock(RequestRegistry.class);
        RequestSchedulerV2 schedulerV2 = mock(RequestSchedulerV2.class);
        RequestScheduler scheduler = scheduler(config, registry, schedulerV2);
        BalanceContext context = org.flexlb.balance.schedulev2.V2TestSupport.request(config, "1", 50, 10).ctx();
        CompletableFuture<Response> future = new CompletableFuture<>();
        RequestState state = state();
        when(schedulerV2.submit(context)).thenReturn(future);
        when(schedulerV2.getRequestState("1", 0L)).thenReturn(state);
        when(schedulerV2.cancelRequest("1", 0L, CancelReason.CLIENT_CANCELLED)).thenReturn(state);

        assertSame(future, scheduler.submit(context));
        assertSame(state, scheduler.getRequestState("1", 0L));
        assertSame(state, scheduler.getRequestState("1", 0L, RequestPhase.GENERATION));
        assertSame(state, scheduler.cancelRequest("1", 0L, CancelReason.CLIENT_CANCELLED));
        assertSame(state, scheduler.cancelRequest("1", 0L, CancelReason.CLIENT_CANCELLED, RequestPhase.GENERATION));
        verify(registry, never()).register(any());

        scheduler.closePlacement();
        verify(schedulerV2).close();
    }

    @Test
    void encoderPhaseQueriesStayOnRequestRegistry() {
        FlexlbConfig config = SchedulingTestConfig.routeConfig();
        config.getDispatcher().setType(DispatcherConfig.Type.NON_BATCH_V2);
        RequestRegistry registry = mock(RequestRegistry.class);
        RequestSchedulerV2 schedulerV2 = mock(RequestSchedulerV2.class);
        RequestScheduler scheduler = scheduler(config, registry, schedulerV2);
        RequestState state = state();
        when(registry.getRequestState("1", 0L, RequestPhase.ENCODER)).thenReturn(state);

        assertSame(state, scheduler.getRequestState("1", 0L, RequestPhase.ENCODER));
        verify(schedulerV2, never()).getRequestState(any(), anyLong());
    }

    private static RequestState state() {
        return new RequestState("1", 1L, RequestState.Phase.QUEUED, DeliveryClaimKind.NONE, 0L, 0L, 0L, null);
    }

    @SuppressWarnings("unchecked")
    private static RequestScheduler scheduler(FlexlbConfig config,
                                              RequestRegistry registry,
                                              RequestSchedulerV2 schedulerV2) {
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        return new RequestScheduler(service, mock(DefaultRouter.class), registry, schedulerV2);
    }
}
