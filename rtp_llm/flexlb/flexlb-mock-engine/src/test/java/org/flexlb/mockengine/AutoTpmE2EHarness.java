package org.flexlb.mockengine;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.scheduler.RequestScheduler;
import org.flexlb.balance.scheduler.RequestSchedulerTestRuntime;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.QueueOrderingConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.client.EngineGrpcClient;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.flexlb.mockengine.MockEngineTestSupport.unary;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared E2E harness for the real FlexLB scheduler, eviction manager,
 * endpoint runtime wired to an in-process Java mock
 * engine cluster.
 *
 * <p>The E2E loop: the mocked {@link EngineGrpcClient#batchEnqueueAsync} answer
 * bridges into the real {@link JavaMockEngineCluster.FastRpcService#enqueueBatch}
 * of the target port, so dispatch, fault injection, prefill→decode handoff and
 * CANCELLED completions all run through real mock-engine code. The WorkerStatus
 * pump reads real {@code getWorkerStatus} snapshots and feeds them through the
 * prepared-status transaction, closing the calibrate/settle loop exactly like
 * production polling would.
 *
 * <p>By default, {@code ConfigService}/{@code DefaultRouter}/{@code EngineGrpcClient}/reporters
 * are Mockito stand-ins — identical to the flexlb-sync unit-test harness pattern.
 * Routing-regression scenarios can opt into the production {@link DefaultRouter}
 * and endpoint-selection strategies while retaining the in-process transport.
 */
final class AutoTpmE2EHarness implements AutoCloseable {

    final FlexlbConfig config;
    final ConfigService configService = mock(ConfigService.class);
    private final java.util.concurrent.atomic.AtomicReference<Throwable> pumpFailure =
            new java.util.concurrent.atomic.AtomicReference<>();
    final EngineGrpcClient grpcClient = mock(EngineGrpcClient.class);
    final BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
    final RequestSchedulerReporter requestReporter = mock(RequestSchedulerReporter.class);

    final Map<Integer, JavaMockEngineCluster.FastRpcService> services = new ConcurrentHashMap<>();
    final List<JavaMockEngineCluster.FastRpcService> prefillEngines = new ArrayList<>();
    final List<JavaMockEngineCluster.FastRpcService> decodeEngines = new ArrayList<>();
    final ScheduledExecutorService engineScheduler = Executors.newScheduledThreadPool(8);

    final EndpointRegistry endpointRegistry;
    final RequestScheduler scheduler;
    private final RequestSchedulerTestRuntime schedulerRuntime;

    /** requestIds in the order the mock engines actually received them via enqueueBatch. */
    final List<Long> engineArrivalOrder = new CopyOnWriteArrayList<>();
    /** requestId -> first enqueueBatch arrival time (nanos), for latency measurements. */
    final Map<Long, Long> engineArrivalNanos = new ConcurrentHashMap<>();

    /** Prefill index chosen by the deterministic fixture route. */
    volatile Function<BalanceContext, Integer> prefillSelector = ctx -> 0;

    private final Map<Integer, WorkerStatus> statusByPort = new ConcurrentHashMap<>();
    private final Map<Integer, String> ipPortByEnginePort = new ConcurrentHashMap<>();
    private final Object pumpLock = new Object();
    private ScheduledExecutorService pumpExecutor;
    private final Path tempDir;

    AutoTpmE2EHarness(int basePort, int nPrefill, int nDecode, String prefillFormulaMs, double decodeStepMs) {
        this.config = new FlexlbConfig();
        try {
            tempDir = Files.createTempDirectory("auto-tpm-e2e");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        MockPerformanceModel model = model(prefillFormulaMs, decodeStepMs);
        // Sizable pools: master-dispatched traffic routes P->D through
        // role_addrs, so since the P-enqueue decode-KV reservation (20260903)
        // every in-flight request holds its D-side lease AT ENQUEUE — a
        // 100-block pool rejects past ~95 concurrent requests on the 5%
        // reserve watermark. These behavioral E2E tests treat KV capacity as
        // a non-constraint (the KV-rejection surface has dedicated tests).
        final int poolBlocks = 6144;
        for (int i = 0; i < nPrefill; i++) {
            int port = basePort + i;
            JavaMockEngineCluster.FastRpcService svc = new JavaMockEngineCluster.FastRpcService(
                    "prefill", EngineRpcService.RoleTypePB.ROLE_TYPE_PREFILL,
                    port, services, engineScheduler, model, poolBlocks,
                    new JavaMockEngineCluster.ClusterStats());
            services.put(port, svc);
            prefillEngines.add(svc);
        }
        for (int i = 0; i < nDecode; i++) {
            int port = basePort + nPrefill + i;
            JavaMockEngineCluster.FastRpcService svc = new JavaMockEngineCluster.FastRpcService(
                    "decode", EngineRpcService.RoleTypePB.ROLE_TYPE_DECODE,
                    port, services, engineScheduler, model, poolBlocks,
                    new JavaMockEngineCluster.ClusterStats());
            services.put(port, svc);
            decodeEngines.add(svc);
        }

        // Conservative defaults; scenarios override before submitting traffic.
        // Priority ordering must be set BEFORE registerEndpoint (PrefillQueue freezes it).
        config.queueScheduler().setOrdering(QueueOrderingConfig.priority());
        config.getDispatcher().setMainQueueConsumerThreads(2);
        config.getDispatcher().setDecodeQueueThreads(2);
        config.getDispatcher().setMaxInflightPerPrefillWorker(64);
        config.getRequestLifecycle().getRequest().setTimeoutMs(3_600_000L);
        config.getRequestLifecycle().getRequest().setStatusUpdateTimeoutMs(3_600_000L);
        config.getRequestLifecycle().getDecision().setLifetime(2.0);
        when(configService.loadBalanceConfig()).thenReturn(config);

        schedulerRuntime = new RequestSchedulerTestRuntime(
                configService,
                reporter,
                requestReporter,
                context -> httpPort(prefillEngines.get(prefillSelector.apply(context)).getGrpcPort()));
        endpointRegistry = schedulerRuntime.endpointRegistry();
        scheduler = schedulerRuntime.scheduler();

        for (JavaMockEngineCluster.FastRpcService svc : prefillEngines) {
            registerEndpoint(RoleType.PREFILL, svc);
        }
        for (JavaMockEngineCluster.FastRpcService svc : decodeEngines) {
            registerEndpoint(RoleType.DECODE, svc);
        }
    }

    // ==================== endpoint / route wiring ====================

    private void registerEndpoint(RoleType role, JavaMockEngineCluster.FastRpcService svc) {
        int grpcPort = svc.getGrpcPort();
        WorkerStatus ws = publishEndpoint(role, svc);
        statusByPort.put(grpcPort, ws);
        ipPortByEnginePort.put(grpcPort, ws.getLogicalIpPort());
    }

    private static int httpPort(int grpcPort) {
        return grpcPort - org.flexlb.constant.CommonConstants.GRPC_PORT_OFFSET;
    }

    private WorkerStatus publishEndpoint(RoleType role, JavaMockEngineCluster.FastRpcService svc) {
        WorkerStatus status = WorkerStatus.createDiscovered(
                role, "g1", "127.0.0.1", httpPort(svc.getGrpcPort()), svc.getGrpcPort(), null);
        // Discovery and polling share the Engine's version sequence. A synthetic
        // version can collide with the first real response and hide completions.
        EngineRpcService.WorkerStatusPB initial = workerStatus(svc, 0L);
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, initial));
            schedulerRuntime.publishEndpoint(status, prepared);
        } finally {
            status.lock.unlock();
        }
        return status;
    }

    PrefillEndpoint prefillEndpoint(int index) {
        return (PrefillEndpoint) endpointRegistry.get(
                RoleType.PREFILL,
                ipPortByEnginePort.get(prefillEngines.get(index).getGrpcPort()));
    }

    DecodeEndpoint decodeEndpoint(int index) {
        return (DecodeEndpoint) endpointRegistry.get(
                RoleType.DECODE,
                ipPortByEnginePort.get(decodeEngines.get(index).getGrpcPort()));
    }

    void setDecodeKvCapacity(int index, long available, long total) {
        synchronized (pumpLock) {
            JavaMockEngineCluster.FastRpcService service = decodeEngines.get(index);
            WorkerStatus status = statusByPort.get(service.getGrpcPort());
            EngineRpcService.WorkerStatusPB response =
                    workerStatus(service, status.appliedStatusCursor().latestFinishedTaskVersion()).toBuilder()
                            .setAvailableKvCache(available)
                            .setTotalKvCache(total)
                            .build();
            schedulerRuntime.applyStatus(status, response);
        }
    }

    void allowPreemption(VictimStage first, VictimStage... additional) {
        PreemptionConfig preemption = new PreemptionConfig();
        EnumSet<VictimStage> stages = EnumSet.of(first, additional);
        preemption.setAllowedVictimStages(stages);
        config.priorityOrdering().setPreemption(preemption);
    }

    private ServerStatus prefillServer(int index, String requestId) {
        int grpcPort = prefillEngines.get(index).getGrpcPort();
        return server(
                RoleType.PREFILL, "127.0.0.1", httpPort(grpcPort), grpcPort, requestId);
    }

    private ServerStatus decodeServer(int index, String requestId) {
        int grpcPort = decodeEngines.get(index).getGrpcPort();
        return server(
                RoleType.DECODE, "127.0.0.1", httpPort(grpcPort), grpcPort,
                requestId);
    }

    /**
     * Deterministic full-route stand-in for queue tests. Transient Decode
     * capacity is deliberately not filtered here: production queue routing
     * selects a physically valid winner first, and the commit boundary turns
     * an exact capacity miss into either a wait or a preemption transaction.
     */
    Response defaultRoute(BalanceContext ctx) {
        int prefillIndex = prefillSelector.apply(ctx);
        ServerStatus prefill = prefillServer(prefillIndex, ctx.getRequestId());
        Response response = new Response();
        response.setSuccess(true);
        response.setServerStatus(List.of(
                prefill,
                decodeServer(0, ctx.getRequestId())));
        return response;
    }

    private static ServerStatus server(
            RoleType role, String ip, int httpPort, int grpcPort, String requestId) {
        ServerStatus status = new ServerStatus();
        status.setSuccess(true);
        status.setRole(role);
        status.setServerIp(ip);
        status.setHttpPort(httpPort);
        status.setGrpcPort(grpcPort);
        status.setDpRank(0);
        status.setGroup("g1");
        status.setRequestId(requestId);
        return status;
    }

    // ==================== request construction ====================

    BalanceContext context(String requestId, int priority) {
        return context(requestId, priority, 128, 8);
    }

    BalanceContext context(long requestId, int priority) {
        return context(Long.toString(requestId), priority);
    }

    BalanceContext context(String requestId, int priority, long seqLen, int maxNewTokens) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setSeqLen(seqLen);
        request.setMaxNewTokens(maxNewTokens);
        request.setNumBeams(1);
        request.setModel("test-model");
        request.setPriority(priority);

        BalanceContext ctx = new BalanceContext(config);
        ctx.setRequest(request);
        ctx.setGenerateInputPb(
                ByteString.copyFrom(generateInputBytes(requestId, (int) seqLen, maxNewTokens)));
        // Mirror production admission with immutable request scheduling metadata.
        // A 30 s request lifetime avoids accidental expiry during eviction tests.
        ctx.setSchedulingMetadata(SchedulingMetadata.explicit(
                priority, ctx.getStartTime() + 30_000));
        return ctx;
    }

    BalanceContext context(
            long requestId, int priority, long seqLen, int maxNewTokens) {
        return context(Long.toString(requestId), priority, seqLen, maxNewTokens);
    }

    CompletableFuture<Response> submitAndGenerate(BalanceContext context) {
        return scheduler.submit(context).thenApply(response -> generate(context, response));
    }

    private Response generate(BalanceContext context, Response response) {
        if (!response.isSuccess()) {
            return response;
        }
        try {
            var input = EngineRpcService.GenerateInputPB.parseFrom(context.getGenerateInputPb()).toBuilder();
            JavaMockEngineCluster.FastRpcService prefill = null;
            for (var status : response.getServerStatus()) {
                RoleType role = status.getRole();
                input.getGenerateConfigBuilder().addRoleAddrs(EngineRpcService.RoleAddrPB.newBuilder()
                        .setRole(org.flexlb.engine.grpc.RoleTypeProtoConverter.toRoleAddrProto(role))
                        .setRoleStr(role.getCode()).setIp(status.getServerIp())
                        .setHttpPort(status.getHttpPort()).setGrpcPort(status.getGrpcPort()));
                if (role == RoleType.PREFILL) {
                    prefill = services.get(status.getGrpcPort());
                }
            }
            if (prefill == null) {
                throw new IllegalStateException("No selected mock Prefill server");
            }
            long requestId = input.getRequestId();
            engineArrivalOrder.add(requestId);
            engineArrivalNanos.putIfAbsent(requestId, System.nanoTime());
            prefill.generateStreamCall(input.build(), new StreamObserver<EngineRpcService.GenerateOutputsPB>() {
                @Override public void onNext(EngineRpcService.GenerateOutputsPB output) { }
                @Override public void onError(Throwable error) {
                    failedStreams.incrementAndGet();
                }
                @Override public void onCompleted() { }
            });
            return response;
        } catch (com.google.protobuf.InvalidProtocolBufferException error) {
            throw new IllegalArgumentException("Invalid test GenerateInputPB", error);
        }
    }

    final java.util.concurrent.atomic.AtomicInteger failedStreams = new java.util.concurrent.atomic.AtomicInteger();

    static byte[] generateInputBytes(String requestId, int inputTokens, int maxNewTokens) {
        EngineRpcService.GenerateInputPB.Builder input = RequestIdFixtures.write(EngineRpcService.GenerateInputPB.newBuilder(), requestId)
                .setGenerateConfig(EngineRpcService.GenerateConfigPB.newBuilder()
                        .setMaxNewTokens(maxNewTokens)
                        .build());
        for (int token = 0; token < inputTokens; token++) {
            input.addTokenIds(token);
        }
        return input.build().toByteArray();
    }

    // ==================== WorkerStatus pump (engine → scheduler feedback) ====================

    /**
     * One pump round: read the real WorkerStatus of every mock engine and feed
     * it into the scheduler + decode endpoint calibrate, advancing the
     * per-engine finished-version cursor exactly like production polling.
     */
    void pumpOnce() {
        synchronized (pumpLock) {
            for (JavaMockEngineCluster.FastRpcService svc : services.values()) {
                pumpEngine(svc);
            }
        }
    }

    void pumpPrefillOnce(int index) {
        synchronized (pumpLock) {
            pumpEngine(prefillEngines.get(index));
        }
    }

    void pumpDecodeOnce(int index) {
        synchronized (pumpLock) {
            pumpEngine(decodeEngines.get(index));
        }
    }

    private void pumpEngine(JavaMockEngineCluster.FastRpcService svc) {
        WorkerStatus status = statusByPort.get(svc.getGrpcPort());
        // Only the canonical applied cursor acknowledges completions. Rejected
        // or same-version snapshots leave records available for the next poll.
        EngineRpcService.WorkerStatusPB snapshot = workerStatus(
                svc, status.appliedStatusCursor().latestFinishedTaskVersion());
        schedulerRuntime.applyStatus(status, snapshot);
    }

    static EngineRpcService.WorkerStatusPB workerStatus(
            JavaMockEngineCluster.FastRpcService svc, long sinceVersion) {
        return unary(observer -> svc.getWorkerStatus(
                EngineRpcService.StatusVersionPB.newBuilder()
                        .setLatestFinishedVersion(sinceVersion)
                        .build(),
                observer));
    }

    void startAutoPump(long intervalMs) {
        if (pumpExecutor != null) {
            return;
        }
        pumpExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "e2e-worker-status-pump");
            t.setDaemon(true);
            return t;
        });
        pumpExecutor.scheduleWithFixedDelay(() -> {
            try {
                pumpOnce();
            } catch (Throwable failure) {
                pumpFailure.compareAndSet(null, failure);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    void assertAutoPumpHealthy() {
        Throwable failure = pumpFailure.get();
        if (failure != null) {
            throw new AssertionError("WorkerStatus pump failed", failure);
        }
    }

    void stopAutoPump() {
        if (pumpExecutor != null) {
            pumpExecutor.shutdownNow();
            pumpExecutor = null;
        }
    }

    // ==================== misc helpers ====================

    static void await(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("await timed out: " + message);
    }

    private MockPerformanceModel model(String prefillFormulaMs, double decodeStepMs) {
        try {
            return MockEngineTestSupport.performanceModel(
                    tempDir, prefillFormulaMs, 1.0, decodeStepMs);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        stopAutoPump();
        schedulerRuntime.close();
        for (JavaMockEngineCluster.FastRpcService svc : services.values()) {
            svc.shutdown();
        }
        engineScheduler.shutdownNow();
    }

}
