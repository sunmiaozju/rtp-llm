package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.config.SchedulerConfig;
import org.flexlb.dao.route.RoleType;
import org.flexlb.dao.route.ServiceRoute;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Contracts for the scheduler runtime callbacks behind RequestScheduler. */
class RequestOrchestratorsTest {

    @Test
    void shutdownOwnsOneStrictCloseOrder() {
        RequestRegistry lifecycle =
                mock(RequestRegistry.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        when(lifecycle.closeAdmissionAndAwaitMutations()).thenReturn(true);

        runtime(lifecycle, registry).shutdown();

        InOrder order = inOrder(lifecycle, registry);
        order.verify(lifecycle).closeAdmissionAndAwaitMutations();
        order.verify(lifecycle).closeOutstandingAndTerminalize();
        order.verify(lifecycle).closeExpiration();
        order.verify(registry).close();
        order.verify(lifecycle).closePublisher();
    }

    @Test
    void shutdownNonOwnerDoesNotRepeatAnyCloseLeaf() {
        RequestRegistry lifecycle =
                mock(RequestRegistry.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        when(lifecycle.closeAdmissionAndAwaitMutations()).thenReturn(false);

        runtime(lifecycle, registry).shutdown();

        verify(lifecycle, never()).closeOutstandingAndTerminalize();
        verify(lifecycle, never()).closeExpiration();
        verify(registry, never()).close();
        verify(lifecycle, never()).closePublisher();
    }

    @Test
    void shutdownPreservesPrimaryFailureButStillExecutesEveryLeaf() {
        RequestRegistry lifecycle =
                mock(RequestRegistry.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        RuntimeException primary = new RuntimeException("outstanding");
        RuntimeException expiration = new RuntimeException("expiration");
        RuntimeException endpoints = new RuntimeException("endpoints");
        RuntimeException publisher = new RuntimeException("publisher");
        when(lifecycle.closeAdmissionAndAwaitMutations()).thenReturn(true);
        doThrow(primary).when(lifecycle).closeOutstandingAndTerminalize();
        doThrow(expiration).when(lifecycle).closeExpiration();
        doThrow(endpoints).when(registry).close();
        doThrow(publisher).when(lifecycle).closePublisher();

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> runtime(lifecycle, registry).shutdown());

        assertSame(primary, thrown);
        assertEquals(3, thrown.getSuppressed().length);
        assertSame(expiration, thrown.getSuppressed()[0]);
        assertSame(endpoints, thrown.getSuppressed()[1]);
        assertSame(publisher, thrown.getSuppressed()[2]);
        verify(lifecycle).closePublisher();
    }

    @Test
    void expirationMaintenancePassesRetentionToDecodeHistoryCleanup() {
        RequestRegistry lifecycle = mock(RequestRegistry.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        doAnswer(invocation -> {
            java.util.function.LongConsumer purgeFinishedDecodeRequests = invocation.getArgument(0);
            purgeFinishedDecodeRequests.accept(123L);
            return null;
        }).when(lifecycle).maintainExpiration(any());

        runtime(lifecycle, registry).maintainExpiration();

        verify(registry).purgeFinishedDecodeRequests(123L);
    }

    @Test
    void v2ReportsConfiguredQueueLimitWithoutLegacyGlobalQueue() {
        ConfigService configs = mock(ConfigService.class);
        FlexlbConfig config = new FlexlbConfig();
        config.getScheduler().setType(SchedulerConfig.Type.QUEUE);
        config.getDispatcher().setType(DispatcherConfig.Type.NON_BATCH_V2);
        config.getScheduler().setMaxQueuedRequests(1000);
        when(configs.loadBalanceConfig()).thenReturn(config);
        RequestRegistry requests = mock(RequestRegistry.class);
        BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
        SchedulerRuntime runtime = new SchedulerRuntime(requests, mock(EndpointRegistry.class), reporter,
                mock(RequestSchedulerReporter.class), configs, new QueueCapacity(),
                new ModelMetaConfig(new ServiceRoute()));

        runtime.report();
        verify(reporter).reportGlobalQueueLimit(1000);
        config.getScheduler().setMaxQueuedRequests(2000);
        runtime.report();
        verify(reporter).reportGlobalQueueLimit(2000);
        verify(reporter, never()).reportGlobalQueue(anyInt(), anyInt());
    }

    @Test
    void metricsDoNothingAfterLifecycleShutdownBegins() {
        RequestRegistry lifecycle =
                mock(RequestRegistry.class);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
        RequestSchedulerReporter admissionReporter =
                mock(RequestSchedulerReporter.class);
        when(lifecycle.isShuttingDown()).thenReturn(true);

        new SchedulerRuntime(lifecycle, registry, reporter, admissionReporter, mock(ConfigService.class),
                new QueueCapacity(), new ModelMetaConfig(new ServiceRoute())).report();

        verify(lifecycle, never()).trackedRequestStats();
    }

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {"PREFILL", "PDFUSION"})
    void sharedQueueReportsOnlyConfiguredRole(RoleType role) {
        FlexlbConfig config = SchedulingTestConfig.newConfig();
        SchedulingTestConfig.useFifoQueue(config);
        config.getScheduler().setMaxQueuedRequests(20);
        config.getDispatcher().setMaxInflightPerPrefillWorker(2);
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        EndpointRegistry registry = mock(EndpointRegistry.class);
        when(registry.getEndpointCount(RoleType.PREFILL)).thenReturn(2);
        when(registry.getEndpointCount(RoleType.PDFUSION)).thenReturn(3);
        QueueCapacity capacity = new QueueCapacity();
        capacity.increaseTotalQueueCount(new CompletableFuture<>());
        capacity.tryReserveEngineSubQueueSlot(config, RoleType.PREFILL, 2);
        capacity.tryReserveEngineSubQueueSlot(config, RoleType.PDFUSION, 3);
        BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
        RequestRegistry requests = mock(RequestRegistry.class);

        ModelMetaConfig modelMetaConfig = mock(ModelMetaConfig.class);
        when(modelMetaConfig.getRequiredRoles()).thenReturn(Set.of(role));
        new SchedulerRuntime(requests, registry, reporter, mock(RequestSchedulerReporter.class),
                service, capacity, modelMetaConfig).report();

        verify(reporter).reportTotalQueueWaiting(1L);
        verify(reporter).reportSharedSubQueue(role.name(), 1, role == RoleType.PREFILL ? 16L : 20L,
                role == RoleType.PREFILL ? 2 : 3);
        RoleType other = role == RoleType.PREFILL ? RoleType.PDFUSION : RoleType.PREFILL;
        verify(reporter, never()).reportSharedSubQueue(eq(other.name()),
                anyInt(), anyLong(), anyInt());
    }

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {"PREFILL", "PDFUSION"})
    void configuredRoleIsReportedEvenWithoutWorkers(RoleType role) {
        FlexlbConfig config = SchedulingTestConfig.newConfig();
        SchedulingTestConfig.useFifoQueue(config);
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        ModelMetaConfig modelMetaConfig = mock(ModelMetaConfig.class);
        when(modelMetaConfig.getRequiredRoles()).thenReturn(Set.of(role));
        BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
        SchedulerRuntime runtime = new SchedulerRuntime(mock(RequestRegistry.class), mock(EndpointRegistry.class),
                reporter, mock(RequestSchedulerReporter.class), service, new QueueCapacity(), modelMetaConfig);
        when(modelMetaConfig.getRequiredRoles()).thenReturn(Set.of());

        runtime.report();

        verify(reporter).reportSharedSubQueue(role.name(), 0, 0L, 0);
        RoleType other = role == RoleType.PREFILL ? RoleType.PDFUSION : RoleType.PREFILL;
        verify(reporter, never()).reportSharedSubQueue(eq(other.name()),
                anyInt(), anyLong(), anyInt());
    }

    private static SchedulerRuntime runtime(RequestRegistry requests, EndpointRegistry endpoints) {
        return new SchedulerRuntime(requests, endpoints, mock(BatchSchedulerReporter.class),
                mock(RequestSchedulerReporter.class), mock(ConfigService.class), new QueueCapacity(),
                new ModelMetaConfig(new ServiceRoute()));
    }

    private static void clearEndpointRegistry() {
        EndpointRegistry.getEndpointsByRole(RoleType.PREFILL).clear();
        EndpointRegistry.getEndpointsByRole(RoleType.PDFUSION).clear();
        EndpointRegistry.getEndpointsByRole(RoleType.DECODE).clear();
    }
}
