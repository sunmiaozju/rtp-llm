package org.flexlb.sync.runner;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.enums.BalanceStatusEnum;
import org.flexlb.service.grpc.EngineGrpcService;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.sync.status.WorkerDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class GrpcWorkerStatusRunnerTest {

    @ParameterizedTest
    @CsvSource({"DECODE,false,false", "PREFILL,true,false", "PDFUSION,true,false", "PREFILL,true,true"})
    void repeatedStatusCheckFailuresRetireWorkerAndOnlyClearDetailedCacheIndexes(RoleType role,
                                                                                 boolean needsCacheKeys,
                                                                                 boolean invalidRole) {
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(
                org.flexlb.balance.scheduler.SchedulingTestConfig.newConfig());
        EndpointRegistry registry = RunnerTestSupport.endpointRegistry(config);
        WorkerStatus status = RunnerTestSupport.discovered(
                role, null, "127.0.0.1", 8080, 8081, "test-site");
        WorkerDirectory directory = directory(registry, status);
        WorkerEndpoint endpoint = RunnerTestSupport.publishEndpoint(
                registry, role, status.getLogicalIpPort(), status);
        CacheAwareService cache = mock(CacheAwareService.class);
        EngineGrpcService grpc = mock(EngineGrpcService.class);
        when(grpc.getWorkerStatusAsync(
                anyString(), anyInt(), anyLong(), anyLong(), any()))
                .thenReturn(invalidRole
                        ? CompletableFuture.completedFuture(EngineRpcService.WorkerStatusPB.newBuilder()
                                 .setRoleType(EngineRpcService.RoleTypePB.ROLE_TYPE_PREFILL)
                                 .setRole("DECODE")
                                 .setStatusVersion(1L).setAlive(true)
                                 .build())
                        : CompletableFuture.failedFuture(io.grpc.Status.UNAVAILABLE.asRuntimeException()));

        var health = config.loadBalanceConfig().getWorkerRegistry().getHealth();
        for (int failure = 1; failure <= 10; failure++) {
            WorkerStatus.PollLease lease = status.tryBeginStatusPoll();
            assertNotNull(lease);
            new GrpcWorkerStatusRunner(status.getLogicalIpPort(), "test-site", role, null, status, lease, directory,
                    mock(EngineHealthReporter.class), grpc, 5_000L, cache, Runnable::run,
                    health::getStatusPollFailureThreshold).run();
            if (failure < 10) {
                assertSame(status, directory.getStatusBy(role).get(status.getLogicalIpPort()));
                assertSame(endpoint, registry.get(role, status.getLogicalIpPort()));
                verifyNoInteractions(cache);
            }
        }

        assertNull(registry.get(role, status.getLogicalIpPort()));
        assertTrue(directory.getStatusBy(role).isEmpty());
        assertNull(status.tryBeginStatusPoll(), "retired generation cannot poll again");
        if (needsCacheKeys) {
            verify(cache).removeEngineBlockCache(status.getLogicalIpPort());
        } else {
            verifyNoInteractions(cache);
        }
    }

    @ParameterizedTest
    @CsvSource({"DEADLINE_EXCEEDED,WORKER_STATUS_GRPC_TIMEOUT", "UNAVAILABLE,WORKER_SERVICE_UNAVAILABLE"})
    void reportsFailedRpcLatencyWithSameWorkerTags(String grpcCode, BalanceStatusEnum expectedError) {
        WorkerStatus status = RunnerTestSupport.discovered(
                RoleType.PREFILL, null, "127.0.0.1", 8080, 8081, "test-site");
        WorkerDirectory directory = directory(mock(EndpointRegistry.class), status);
        EngineGrpcService grpc = mock(EngineGrpcService.class);
        io.grpc.Status failure = "DEADLINE_EXCEEDED".equals(grpcCode)
                ? io.grpc.Status.DEADLINE_EXCEEDED : io.grpc.Status.UNAVAILABLE;
        when(grpc.getWorkerStatusAsync(anyString(), anyInt(), anyLong(), anyLong(), any()))
                .thenReturn(CompletableFuture.failedFuture(failure.asRuntimeException()));
        EngineHealthReporter reporter = mock(EngineHealthReporter.class);

        new GrpcWorkerStatusRunner(status.getLogicalIpPort(), "test-site", RoleType.PREFILL, null, status,
                status.tryBeginStatusPoll(), directory, reporter, grpc, 5000L, mock(CacheAwareService.class),
                Runnable::run, () -> 10).run();

        ArgumentCaptor<Long> latency = ArgumentCaptor.forClass(Long.class);
        verify(reporter).reportStatusCheckerFail(expectedError, status.getMetricIpPort(), RoleType.PREFILL);
        verify(reporter).reportStatusCheckFailureLatency(org.mockito.ArgumentMatchers.eq(RoleType.PREFILL),
                latency.capture());
        assertTrue(latency.getValue() >= 0L);
    }

    @Test
    void reportsEachObservedStepOnceAcrossPolls() {
        WorkerStatus status = RunnerTestSupport.discovered(
                RoleType.DECODE, null, "127.0.0.1", 8080, 8081, "test-site");
        String ipPort = status.getLogicalIpPort();
        WorkerEndpoint endpoint = mock(WorkerEndpoint.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        WorkerDirectory directory = directory(registry, status);
        when(registry.publishPreparedEndpoint(anyString(), any(), any())).thenAnswer(invocation -> {
            status.publishPreparedStatus(invocation.getArgument(2));
            return new EndpointRegistry.EndpointPublication(endpoint, () -> { });
        });
        when(registry.get(RoleType.DECODE, ipPort, status)).thenReturn(endpoint);
        when(endpoint.applyPreparedStatus(any(), any())).thenAnswer(invocation -> {
            status.publishPreparedStatus(invocation.getArgument(1));
            return (Runnable) () -> { };
        });
        when(endpoint.observeStatusHeartbeat(any(), any())).thenReturn(() -> { });
        EngineGrpcService grpc = mock(EngineGrpcService.class);
        EngineHealthReporter reporter = mock(EngineHealthReporter.class);
        long[] versions = {1, 2, 2, 3, 4};
        long[] steps = {0, 42, 42, 42, 43};
        for (int i = 0; i < versions.length; i++) {
            var response = EngineRpcService.WorkerStatusPB.newBuilder()
                    .setRoleType(EngineRpcService.RoleTypePB.ROLE_TYPE_DECODE)
                    .setStatusVersion(versions[i]).setAlive(true);
            if (steps[i] > 0) {
                response.setLastStepMetrics(EngineRpcService.WorkerStepMetricsPB.newBuilder()
                        .setStepId(steps[i]).setTotalScheduledTokens(64).setTokenBudget(32000)
                        .setBudgetFillRatio(0.002));
            }
            when(grpc.getWorkerStatusAsync(anyString(), anyInt(), anyLong(), anyLong(), any()))
                    .thenReturn(CompletableFuture.completedFuture(response.build()));
            new GrpcWorkerStatusRunner(ipPort, "test-site", RoleType.DECODE, null, status, status.tryBeginStatusPoll(),
                    directory, reporter, grpc, 5000L, mock(CacheAwareService.class), Runnable::run, () -> 10).run();
        }
        var observed = ArgumentCaptor.forClass(WorkerStatus.StepMetrics.class);
        verify(reporter, org.mockito.Mockito.times(2)).recordWorkerStepMetrics(org.mockito.Mockito.eq(status),
                observed.capture());
        assertEquals(java.util.List.of(42L, 43L),
                observed.getAllValues().stream().map(WorkerStatus.StepMetrics::stepId).toList());
    }

    @Test
    void newGenerationStatusNotifierRunsOutsideWorkerStatusLock() {
        WorkerStatus status = RunnerTestSupport.discovered(
                RoleType.DECODE, null, "127.0.0.1",
                8080, 8081, "test-site");
        String ipPort = status.getLogicalIpPort();
        WorkerStatus.PollLease pollLease = status.tryBeginStatusPoll();
        assertNotNull(pollLease);

        AtomicBoolean publicationHeldLock = new AtomicBoolean();
        AtomicBoolean notifierReleasedLock = new AtomicBoolean();
        WorkerEndpoint endpoint = mock(WorkerEndpoint.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        WorkerDirectory directory = directory(registry, status);
        when(registry.publishPreparedEndpoint(
                org.mockito.Mockito.eq(ipPort),
                org.mockito.Mockito.eq(status),
                any(WorkerStatus.PreparedStatus.class)))
                .thenAnswer(invocation -> {
                    publicationHeldLock.set(
                            status.lock.isHeldByCurrentThread());
                    status.publishPreparedStatus(invocation.getArgument(2));
                    return new EndpointRegistry.EndpointPublication(
                            endpoint,
                            () -> notifierReleasedLock.set(
                                    !status.lock.isHeldByCurrentThread()));
                });

        EngineRpcService.WorkerStatusPB response =
                EngineRpcService.WorkerStatusPB.newBuilder()
                        .setRole(RoleType.DECODE.getCode())
                        .setRoleType(
                                EngineRpcService.RoleTypePB.ROLE_TYPE_DECODE)
                        .setStatusVersion(1L)
                        .setAlive(true)
                        .build();
        EngineGrpcService grpc = mock(EngineGrpcService.class);
        when(grpc.getWorkerStatusAsync(
                anyString(), anyInt(), anyLong(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(response));

        new GrpcWorkerStatusRunner(ipPort, "test-site", RoleType.DECODE, null, status, pollLease, directory,
                mock(EngineHealthReporter.class), grpc, 5_000L, mock(CacheAwareService.class), Runnable::run, () -> 10)
                .run();

        assertTrue(publicationHeldLock.get(),
                "publication must commit under the generation lock");
        assertTrue(notifierReleasedLock.get(),
                "endpoint facts must be handled after releasing that lock");
    }

    @Test
    void sameVersionResponseNotifiesExactEndpointActivity() {
        WorkerStatus status = RunnerTestSupport.alive(
                RoleType.DECODE, null, "127.0.0.1",
                8080, 8081, "test-site");
        String ipPort = status.getLogicalIpPort();
        WorkerStatus.PollLease pollLease = status.tryBeginStatusPoll();
        assertNotNull(pollLease);

        WorkerEndpoint endpoint = mock(WorkerEndpoint.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        WorkerDirectory directory = directory(registry, status);
        when(registry.get(RoleType.DECODE, ipPort, status))
                .thenReturn(endpoint);
        java.util.concurrent.atomic.AtomicBoolean notified =
                new java.util.concurrent.atomic.AtomicBoolean();
        Runnable activity = () -> notified.set(true);
        when(endpoint.observeStatusHeartbeat(any(), any()))
                .thenReturn(activity);

        EngineRpcService.TaskInfoPB task = EngineRpcService.TaskInfoPB.newBuilder()
                .setRequestId(123)
                .setPhase(EngineRpcService.TaskPhase.TASK_PHASE_RUNNING)
                .build();
        EngineRpcService.WorkerStatusPB response =
                EngineRpcService.WorkerStatusPB.newBuilder()
                        .setRole(RoleType.DECODE.getCode())
                        .setRoleType(EngineRpcService.RoleTypePB.ROLE_TYPE_DECODE)
                        .setStatusVersion(
                                status.appliedStatusCursor().statusVersion())
                        .setAlive(true)
                        .addRunningTaskInfo(task)
                        .build();
        EngineGrpcService grpc = mock(EngineGrpcService.class);
        when(grpc.getWorkerStatusAsync(
                anyString(), anyInt(), anyLong(), anyLong(), any()))
                .thenReturn(CompletableFuture.completedFuture(response));
        new GrpcWorkerStatusRunner(ipPort, "test-site", RoleType.DECODE, null, status, pollLease, directory,
                mock(EngineHealthReporter.class), grpc, 5_000L, mock(CacheAwareService.class), Runnable::run, () -> 10)
                .run();

        ArgumentCaptor<WorkerStatus.StatusObservation> observation =
                ArgumentCaptor.forClass(WorkerStatus.StatusObservation.class);
        verify(endpoint).observeStatusHeartbeat(
                org.mockito.Mockito.eq(status), observation.capture());
        assertTrue(observation.getValue().activeTasks().values().stream()
                .anyMatch(active -> active.requestId().equals("123")));
        assertTrue(notified.get());
        WorkerStatus.PollLease nextPoll = status.tryBeginStatusPoll();
        assertNotNull(nextPoll, "the asynchronous owner must close the exact poll lease");
        nextPoll.close();
    }

    @Test
    void pp0RoutingWaitsForThePodToConfirmItselfAndRetiresItWhenUnhealthy() {
        WorkerDirectory directory = prefillDirectory();
        WorkerStatus status = discoverPrefill(directory, "192.0.2.10", true);
        String address = status.getLogicalIpPort();
        WorkerStatus.AppliedStatusCursor initialCursor = status.appliedStatusCursor();

        for (String pp0Ip : java.util.List.of("", "192.0.2.11", "192.0.2.10:8081")) {
            poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, true, 20L, pp0Ip)));
            assertNull(directory.findEndpoint(RoleType.PREFILL, address));
            assertEquals(initialCursor, status.appliedStatusCursor());
        }

        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, true, 1L, status.getIp())));

        assertNotNull(directory.findEndpoint(RoleType.PREFILL, address));
        assertEquals(1L, status.appliedStatusCursor().statusVersion());

        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, false, 2L, status.getIp())));

        assertNull(directory.findEndpoint(RoleType.PREFILL, address));
    }

    @Test
    void followerStatusAndRpcFailureKeepTheHeadRoutable() {
        WorkerDirectory directory = prefillDirectory();
        WorkerStatus head = discoverPrefill(directory, "192.0.2.20", true);
        WorkerStatus follower = discoverPrefill(directory, "192.0.2.21", true);
        WorkerStatus.AppliedStatusCursor followerCursor = follower.appliedStatusCursor();
        poll(directory, head, CompletableFuture.completedFuture(pp0Response(head, true, 1L, head.getIp())));
        WorkerEndpoint headEndpoint = directory.findEndpoint(RoleType.PREFILL, head.getLogicalIpPort());
        assertNotNull(headEndpoint);

        poll(directory, follower, CompletableFuture.completedFuture(pp0Response(follower, true, 20L, head.getIp())));
        assertEquals(followerCursor, follower.appliedStatusCursor());
        assertNull(directory.findEndpoint(RoleType.PREFILL, follower.getLogicalIpPort()));
        assertEquals(java.util.List.of(head),
                java.util.List.copyOf(directory.getWorkerStatuses(RoleType.PREFILL, null)));

        poll(directory, follower, CompletableFuture.failedFuture(io.grpc.Status.UNAVAILABLE.asRuntimeException()));

        assertSame(headEndpoint, directory.findEndpoint(RoleType.PREFILL, head.getLogicalIpPort()));
        poll(directory, head, CompletableFuture.completedFuture(pp0Response(head, false, 2L, head.getIp())));
        assertNull(directory.findEndpoint(RoleType.PREFILL, head.getLogicalIpPort()));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void failedPipelineStopsRoutingToItsPp0WhenHeadReportsUnhealthy(int failedStage) {
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(SchedulingTestConfig.newConfig());
        EndpointRegistry registry = RunnerTestSupport.endpointRegistry(config);
        try {
            WorkerDirectory directory = new WorkerDirectory(registry);
            int firstHost = 50 + failedStage * 10;
            WorkerStatus affectedHead = discoverPrefill(directory, "192.0.2." + firstHost, true);
            List<WorkerStatus> followers = List.of(discoverPrefill(directory, "192.0.2." + (firstHost + 1), true),
                    discoverPrefill(directory, "192.0.2." + (firstHost + 2), true),
                    discoverPrefill(directory, "192.0.2." + (firstHost + 3), true));
            WorkerStatus healthyHead = discoverPrefill(directory, "192.0.2." + (firstHost + 4), true);
            poll(directory, affectedHead,
                    CompletableFuture.completedFuture(pp0Response(affectedHead, true, 1L, affectedHead.getIp())));
            for (WorkerStatus follower : followers) {
                poll(directory, follower,
                        CompletableFuture.completedFuture(pp0Response(follower, true, 1L, affectedHead.getIp())));
            }
            var healthyStatus = pp0Response(healthyHead, true, 1L, healthyHead.getIp()).toBuilder()
                    .setDpMasterAddress(healthyHead.getIp())
                    .build();
            poll(directory, healthyHead, CompletableFuture.completedFuture(healthyStatus));
            assertEquals(Set.of(affectedHead.getLogicalIpPort(), healthyHead.getLogicalIpPort()),
                    Set.copyOf(directory.getPrefillCandidates(RoleType.PREFILL)
                            .stream()
                            .map(EndpointRegistry.PrefillRoutingEntry::address)
                            .toList()));

            poll(directory, followers.get(failedStage - 1),
                    CompletableFuture.failedFuture(io.grpc.Status.UNAVAILABLE.asRuntimeException()));
            poll(directory, affectedHead,
                    CompletableFuture.completedFuture(pp0Response(affectedHead, false, 2L, affectedHead.getIp())));

            assertEquals(List.of(healthyHead.getLogicalIpPort()),
                    directory.getPrefillCandidates(RoleType.PREFILL)
                            .stream()
                            .map(EndpointRegistry.PrefillRoutingEntry::address)
                            .toList());
            poll(directory, healthyHead, CompletableFuture
                    .completedFuture(healthyStatus.toBuilder().setAlive(false).setStatusVersion(2L).build()));
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "192.0.2.32"})
    void sameVersionKeepsConfirmedPp0AndStillChecksHealth(String reportedPp0Ip) {
        WorkerDirectory directory = prefillDirectory();
        WorkerStatus status = discoverPrefill(directory, "192.0.2.30", true);
        String address = status.getLogicalIpPort();
        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, true, 1L, status.getIp())));
        WorkerEndpoint endpoint = directory.findEndpoint(RoleType.PREFILL, address);
        assertNotNull(endpoint);

        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, true, 1L, reportedPp0Ip)));

        assertSame(endpoint, directory.findEndpoint(RoleType.PREFILL, address));

        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, false, 1L, reportedPp0Ip)));

        assertNull(directory.findEndpoint(RoleType.PREFILL, address));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "192.0.2.32"})
    void registeredPp0WithInvalidIdentityRetiresItsDpGroup(String reportedPp0Ip) {
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(SchedulingTestConfig.newConfig());
        EndpointRegistry registry = RunnerTestSupport.endpointRegistry(config);
        try {
            WorkerDirectory directory = new WorkerDirectory(registry);
            WorkerStatus firstRank = discoverPrefill(directory, "192.0.2.30", true);
            WorkerStatus secondRank = discoverPrefill(directory, "192.0.2.31", true);
            poll(directory, firstRank, CompletableFuture.completedFuture(
                    pp0Response(firstRank, true, 1L, firstRank.getIp()).toBuilder().setDpSize(2).build()));
            poll(directory, secondRank,
                    CompletableFuture.completedFuture(pp0Response(secondRank, true, 1L, secondRank.getIp()).toBuilder()
                            .setDpSize(2)
                            .setDpRank(1)
                            .build()));
            assertNotNull(directory.findEndpoint(RoleType.PREFILL, firstRank.getLogicalIpPort()));
            assertNotNull(directory.findEndpoint(RoleType.PREFILL, secondRank.getLogicalIpPort()));

            poll(directory, firstRank, CompletableFuture
                    .completedFuture(pp0Response(firstRank, true, 2L, reportedPp0Ip).toBuilder().setDpSize(2).build()));

            assertNull(directory.findEndpoint(RoleType.PREFILL, firstRank.getLogicalIpPort()));
            assertNull(directory.findEndpoint(RoleType.PREFILL, secondRank.getLogicalIpPort()));
        } finally {
            registry.close();
        }
    }

    @Test
    void disabledPp0RoutingAcceptsLegacyStatusWithoutPp0Identity() {
        WorkerDirectory directory = prefillDirectory();
        WorkerStatus status = discoverPrefill(directory, "192.0.2.40", false);

        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, true, 1L, "")));

        assertNotNull(directory.findEndpoint(RoleType.PREFILL, status.getLogicalIpPort()));
        poll(directory, status, CompletableFuture.completedFuture(pp0Response(status, false, 2L, "")));
    }

    private static WorkerDirectory prefillDirectory() {
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(SchedulingTestConfig.newConfig());
        return new WorkerDirectory(RunnerTestSupport.endpointRegistry(config));
    }

    private static WorkerStatus discoverPrefill(WorkerDirectory directory, String ip, boolean pp0RoutingEnabled) {
        WorkerStatus status = WorkerStatus.createDiscovered(RoleType.PREFILL, null, ip, 8080, 8081, "test-site", null,
                0, 1, pp0RoutingEnabled);
        return directory.currentOrDiscover(RoleType.PREFILL, status.getLogicalIpPort(), () -> status);
    }

    private static EngineRpcService.WorkerStatusPB pp0Response(WorkerStatus status,
                                                               boolean alive,
                                                               long version,
                                                               String pp0Ip) {
        return RunnerTestSupport.response(status, alive, version)
                .setDpSize(1)
                .setDpMasterAddress("192.0.2.1")
                .setLatestFinishedVersion(version)
                .setPp0Ip(pp0Ip)
                .build();
    }

    private static void poll(WorkerDirectory directory,
                             WorkerStatus status,
                             CompletableFuture<EngineRpcService.WorkerStatusPB> response) {
        EngineGrpcService grpc = mock(EngineGrpcService.class);
        when(grpc.getWorkerStatusAsync(anyString(), anyInt(), anyLong(), anyLong(), any())).thenReturn(response);
        new GrpcWorkerStatusRunner(status.getLogicalIpPort(), status.getGrpcPort(), "test-site", status.getRole(),
                status.getGroup(), status, status.tryBeginStatusPoll(), directory, mock(EngineHealthReporter.class),
                grpc, 5000L, mock(CacheAwareService.class), Runnable::run, () -> 1).run();
    }

    private static WorkerDirectory directory(
            EndpointRegistry registry, WorkerStatus status) {
        WorkerDirectory directory = new WorkerDirectory(registry);
        directory.currentOrDiscover(
                status.getRole(), status.getLogicalIpPort(), () -> status);
        return directory;
    }
}
