package org.flexlb.httpserver;

import io.grpc.stub.StreamObserver;
import org.flexlb.cache.domain.CacheMatchQuery;
import org.flexlb.cache.domain.CacheMatchSource;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.cache.match.CacheHitFeedbackTracker;
import org.flexlb.cache.match.CacheMatchFailoverManager;
import org.flexlb.cache.match.CacheMatchQueryOrchestrator;
import org.flexlb.cache.match.CacheMetadataUpdateOrchestrator;
import org.flexlb.cache.match.kvcm.KvcmCacheMatchProvider;
import org.flexlb.cache.match.localstandby.LocalStandbyCacheManager;
import org.flexlb.cache.match.localstandby.LocalStandbyCacheMatchProvider;
import org.flexlb.cache.match.localstandby.LocalStandbyComparisonService;
import org.flexlb.cache.match.localsync.LocalSyncCacheMatchProvider;
import org.flexlb.cache.telemetry.CacheMetricsReporter;
import org.flexlb.config.CacheMatchConfiguration;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.consistency.LBStatusConsistencyService;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusProvider;
import org.flexlb.dao.route.RoleType;
import org.flexlb.dao.route.ServiceRoute;
import org.flexlb.engine.grpc.client.KvcmGrpcClient;
import org.flexlb.schedule.grpc.FlexlbScheduleProtocol;
import org.flexlb.service.RouteService;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlexlbScheduleLocalStandbyTest {

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {"PREFILL", "PDFUSION"})
    void localStandbyMatchesKeysFromSuccessfulScheduleWithoutKvcmTopology(RoleType role) throws Exception {
        ConfigService configs = new ConfigService("""
                {"schemaVersion":3,"requestLifecycle":{"request":{"timeoutMs":100000}},
                 "cacheMatching":{"type":"LOCAL_STANDBY"}}
                """);
        CacheMatchConfiguration configuration = new CacheMatchConfiguration(new ModelMetaConfig(new ServiceRoute()), configs);
        WorkerStatus worker = mock(WorkerStatus.class);
        when(worker.getLogicalIpPort()).thenReturn("127.0.0.1:8080@1");
        WorkerStatusProvider workers = mock(WorkerStatusProvider.class);
        when(workers.getWorkerStatuses(role, "default")).thenReturn(List.of(worker));
        CacheMetricsReporter metrics = mock(CacheMetricsReporter.class);
        LocalStandbyCacheManager manager = spy(new LocalStandbyCacheManager(configuration, workers, metrics));
        CompletableFuture<Void> indexed = new CompletableFuture<>();
        doAnswer(invocation -> {
            invocation.callRealMethod();
            indexed.complete(null);
            return null;
        }).when(manager).addRoutedRequestBlocks(any(), any());
        LocalStandbyCacheMatchProvider provider = new LocalStandbyCacheMatchProvider(
                configuration, manager, metrics);
        var sources = new CacheMatchFailoverManager(configuration, mock(KvcmGrpcClient.class), metrics, configs);
        var queries = new CacheMatchQueryOrchestrator(mock(LocalSyncCacheMatchProvider.class), mock(KvcmCacheMatchProvider.class),
                provider, manager, sources, new LocalStandbyComparisonService(configuration, provider), metrics, configuration);
        CacheAwareService cache = new CacheAwareService(queries, new CacheHitFeedbackTracker(null, configs),
                new CacheMetadataUpdateOrchestrator(configuration, null, provider));
        RouteService router = mock(RouteService.class);
        CompletableFuture<Response> routed = new CompletableFuture<>();
        when(router.route(any())).thenReturn(routed);
        StreamObserver<FlexlbScheduleProtocol.FlexlbScheduleResponsePB> observer = mock(StreamObserver.class);
        try {
            service(router, cache).schedule(request(), observer);
            verify(observer, never()).onCompleted();
            assertEquals(0, manager.mappingCount());

            routed.complete(response(role, true));
            verify(observer).onCompleted();
            verify(observer).onNext(org.mockito.ArgumentMatchers.argThat(result -> result.getSuccess()));
            indexed.get(5, TimeUnit.SECONDS);
            assertEquals(3, manager.mappingCount());
            var prediction = cache.findMatchingEngines(new CacheMatchQuery(
                    "followup", List.of(11L, 22L, 33L), 4096, role, "default"));
            assertEquals(CacheMatchSource.LOCAL_STANDBY, prediction.source());
            assertEquals(CacheMatchSource.LOCAL_STANDBY, queries.status().effectiveSource());
            assertEquals(3, prediction.exactHostMatch(worker.getLogicalIpPort()).localMatchBlocks());
            assertEquals(4096, prediction.blockSize());
            verify(metrics, never()).reportStandbyFallback(any());
        } finally {
            provider.shutdown();
            manager.shutdown();
        }
    }

    @Test
    void failedScheduleDoesNotUpdateIndex() {
        RouteService router = mock(RouteService.class);
        when(router.route(any())).thenReturn(CompletableFuture.completedFuture(response(RoleType.PREFILL, false)));
        CacheAwareService cache = mock(CacheAwareService.class);
        StreamObserver<FlexlbScheduleProtocol.FlexlbScheduleResponsePB> observer = mock(StreamObserver.class);

        service(router, cache).schedule(request(), observer);

        verify(observer).onCompleted();
        verify(cache, never()).updateFromRoutedRequest(any(), any());
    }

    @Test
    void metadataUpdateFailurePreservesSuccessfulSchedule() {
        RouteService router = mock(RouteService.class);
        when(router.route(any())).thenReturn(CompletableFuture.completedFuture(response(RoleType.PREFILL, true)));
        CacheAwareService cache = mock(CacheAwareService.class);
        doThrow(new IllegalStateException("metadata update unavailable")).when(cache).updateFromRoutedRequest(any(), any());
        StreamObserver<FlexlbScheduleProtocol.FlexlbScheduleResponsePB> observer = mock(StreamObserver.class);

        service(router, cache).schedule(request(), observer);

        verify(observer).onNext(org.mockito.ArgumentMatchers.argThat(result -> result.getSuccess()));
        verify(observer).onCompleted();
        verify(observer, never()).onError(any());
    }

    private FlexlbServiceImpl service(RouteService router, CacheAwareService cache) {
        ConfigService config = mock(ConfigService.class);
        FlexlbConfig routingConfig = new FlexlbConfig();
        routingConfig.setDispatcher(new org.flexlb.config.DispatcherConfig());
        when(config.loadBalanceConfig()).thenReturn(routingConfig);
        return new FlexlbServiceImpl(router, mock(LBStatusConsistencyService.class),
                mock(EngineHealthReporter.class), mock(FlexlbGrpcForwarder.class), config,
                mock(BatchSchedulerReporter.class), mock(ServerScheduleLatencyRecorder.class),
                mock(RequestSchedulerReporter.class), cache);
    }

    private FlexlbScheduleProtocol.FlexlbScheduleRequestPB request() {
        return FlexlbScheduleProtocol.FlexlbScheduleRequestPB.newBuilder()
                .setRequestIdStr("warmup").setSeqLen(12288)
                .setCacheKeyBlockSize(4096).addAllBlockCacheKeys(List.of(11L, 22L, 33L)).build();
    }

    private Response response(RoleType role, boolean success) {
        ServerStatus worker = new ServerStatus();
        worker.setSuccess(true);
        worker.setRole(role);
        worker.setServerIp("127.0.0.1");
        worker.setHttpPort(8080);
        worker.setGrpcPort(8081);
        worker.setGroup("default");
        worker.setRequestId("warmup");
        worker.setSelectedEngineIndex(1, 2);
        Response response = new Response();
        response.setSuccess(success);
        response.setServerStatus(List.of(worker));
        return response;
    }
}
