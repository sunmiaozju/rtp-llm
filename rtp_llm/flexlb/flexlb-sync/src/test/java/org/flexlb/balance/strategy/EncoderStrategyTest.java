package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.EncoderEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.sync.status.WorkerDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EncoderStrategyTest {
    private final WorkerDirectory directory = mock(WorkerDirectory.class);
    private final EncoderStrategy strategy = new EncoderStrategy(directory);

    @org.junit.jupiter.api.BeforeEach
    @AfterEach
    void clearRegistry() {
        EndpointRegistry.getEndpointsByRole(RoleType.ENCODER).clear();
    }

    @Test
    void selectsLeastConcurrentWorkerWithoutUsingAvailableKvCache() {
        endpoint("busy", 8001, true, 40, 2, 2, 0);
        EncoderEndpoint zeroKv = endpoint("zero-kv", 8004, true, 0, 0, 1, 0);
        endpoint("small-kv", 8002, true, 10, 1, 1, 0);
        endpoint("winner", 8003, true, 30, 1, 1, 0);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("busy", "zero-kv", "small-kv", "winner"));

        assertSame(zeroKv, strategy.select(context(), "group"));
    }

    @Test
    void ignoresDeadWorkersAndIncludesPendingLocalRequests() {
        endpoint("dead", 8001, false, 100, 0, 0, 0);
        endpoint("negative", 8002, true, -1, 2, 0, 0);
        endpoint("pending", 8003, true, 100, 0, 0, 3);
        EncoderEndpoint winner = endpoint("winner", 8004, true, 1, 1, 0, 0);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("dead", "negative", "pending", "winner"));

        assertSame(winner, strategy.select(context(), "group"));
    }

    @Test
    void returnsNoSelectionWhenNoLiveWorkerExists() {
        endpoint("dead", 8001, false, -1, 0, 0, 0);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("dead"));

        assertNull(strategy.select(context(), "group"));
    }

    @Test
    void selectsZeroKvWorkerWhenItHasLessConcurrentWork() {
        endpoint("positive", 8001, true, 100, 1, 0, 0);
        EncoderEndpoint zero = endpoint("zero", 8002, true, 0, 0, 0, 0);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("positive", "zero"));

        assertSame(zero, strategy.select(context(), "group"));
    }

    @Test
    void knownCacheEstimateSelectsLessUncachedWorkEvenWhenItHasMoreRequests() {
        EncoderEndpoint longJob = endpoint("long-job", 8001, true, 0, 1, 0, 0);
        EncoderEndpoint shortJobs = endpoint("short-jobs", 8002, true, -1, 4, 0, 0);
        when(longJob.inflightUncachedTokenEstimate()).thenReturn(1000L);
        when(shortJobs.inflightUncachedTokenEstimate()).thenReturn(40L);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("long-job", "short-jobs"));

        assertSame(shortJobs, strategy.select(context(1000L, 200L), "group"));
    }

    @Test
    void queueModeSkipsWorkersAtEncoderInflightLimit() {
        endpoint("at-cap", 8001, true, 0, 1, 1, 0);
        EncoderEndpoint winner = endpoint("winner", 8002, true, 0, 1, 0, 0);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("at-cap", "winner"));

        BalanceContext context = context();
        context.getConfig().getDispatcher().setMaxInflightPerEncoderWorker(2);

        assertSame(winner, strategy.select(context, "group"));
    }

    @Test
    void queueModeReturnsNullWhenEveryWorkerIsAtEncoderInflightLimit() {
        endpoint("running", 8001, true, 0, 1, 0, 0);
        endpoint("pending", 8002, true, 0, 0, 0, 1);
        when(directory.endpointAddressView(RoleType.ENCODER))
                .thenReturn(List.of("running", "pending"));

        BalanceContext context = context();
        context.getConfig().getDispatcher().setMaxInflightPerEncoderWorker(1);

        assertNull(strategy.select(context, "group"));
    }

    private EncoderEndpoint endpoint(String address,
                                     int port,
                                     boolean alive,
                                     long kv,
                                     long running,
                                     long waiting,
                                     int pending) {
        EncoderEndpoint endpoint = mock(EncoderEndpoint.class);
        WorkerStatus status = mock(WorkerStatus.class);
        WorkerStatus.EngineObservation engine = mock(WorkerStatus.EngineObservation.class);
        when(directory.findEndpoint(RoleType.ENCODER, address)).thenReturn(endpoint);
        when(endpoint.getStatus()).thenReturn(status);
        when(endpoint.pendingEncoderRequestCount()).thenReturn(pending);
        when(status.isAlive()).thenReturn(alive);
        when(status.topologySnapshot()).thenReturn(new WorkerStatus.TopologySnapshot(
                "group", address, port, port + 1, "site"));
        when(status.committedEngineObservation()).thenReturn(engine);
        when(engine.availableKvCacheTokens()).thenReturn(kv);
        when(engine.runningQueryLen()).thenReturn(running);
        when(engine.waitingQueryLen()).thenReturn(waiting);
        EndpointRegistry.getEndpointsByRole(RoleType.ENCODER).put(address, endpoint);
        return endpoint;
    }

    private static BalanceContext context() {
        BalanceContext context = new BalanceContext();
        Request request = new Request();
        request.setRequestId("encoder-request");
        context.setRequest(request);
        return context;
    }

    private static BalanceContext context(long seqLen, long encoderCacheHitLen) {
        BalanceContext context = context();
        context.getRequest().setSeqLen(seqLen);
        context.getRequest().setEncoderCacheHitLen(encoderCacheHitLen);
        return context;
    }
}
