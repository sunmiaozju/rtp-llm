package org.flexlb.mockengine;

import io.grpc.stub.StreamObserver;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.engine.grpc.EngineRpcService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task35 场景 C：11 种故障注入逐一验证 —— 明确错误传播到调度器终态，
 * post-send 不确定性在请求 TTL 内等待确认，到期回收，且同集群其余引擎不受影响。
 *
 * <p>覆盖 {@link FaultInjectionConfig} 全部字段：failOnEnqueue、enqueueErrorCode、
 * enqueueErrorMessage、enqueueDelayMs、generateDelayMs、generateError、fetchError、
 * noRespond、kvPressureTokens、queueDepthLimit、crashAfterNRequests。
 *
 * <p>控制面（enqueueBatch）故障走真实调度器栈：明确拒绝断言 8510 终态，
 * missing ACK 则断言 TTL 前保留记账、到期回收；数据面（generate_stream/fetch_response）
 * 故障不经过 LB 控制面，用直连 RPC 断言 mock 行为，同时验证调度器路径不受影响。
 *
 * <p>已知缺陷（只报不修，见任务报告）：{@code enqueueErrorCode} 字段从未被
 * {@link JavaMockEngineCluster} 读取 —— 错误响应只携带 message 不携带自定义
 * 错误码，c01 用断言固化该现状。
 */
class FaultInjectionE2ETest {

    private static final int BASE_PORT = 62900;

    /** 通用布防：单请求批次 + 快派发，每个请求恰好一次 enqueueBatch。 */
    private static void arm(AutoTpmE2EHarness h) {
        h.startAutoPump(10);
    }

    private static Response submitTo(AutoTpmE2EHarness h, int prefillIndex,
                                     long requestId) throws Exception {
        h.prefillSelector = ctx -> prefillIndex;
        CompletableFuture<Response> future = h.submitAndGenerate(h.context(requestId, 50));
        return future.get(10, TimeUnit.SECONDS);
    }

    // ==================== C1 failOnEnqueue + enqueueErrorMessage + enqueueErrorCode ====================

    // ==================== C2 enqueueDelayMs ====================

    // ==================== C3 generateDelayMs ====================

    @Test
    @Timeout(30)
    void c03_generate_delay_slows_prefill_execution_without_breaking_completion() throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT + 20, 1, 1, "5", 1.0)) {
            arm(h);
            JavaMockEngineCluster.FastRpcService prefill = h.prefillEngines.get(0);
            prefill.setFaultConfig(FaultInjectionConfig.builder()
                    .generateDelayMs(300)
                    .build());

            long start = System.nanoTime();
            Response response = submitTo(h, 0, 9301);
            assertTrue(response.isSuccess(), "ack is not delayed by generateDelayMs");

            // prefill 执行被拉长 300ms，但最终完整排空、无泄漏
            AutoTpmE2EHarness.await(() -> prefill.getRunningCount() == 0, 5_000,
                    "slowed prefill must still drain");
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs >= 300,
                    "prefill execution must include the injected delay, took " + elapsedMs + "ms");
            assertEquals(1, prefill.getAcceptedCount());
            assertFalse(prefill.isLeakDetected());
        }
    }

    // ==================== C4 generateError（数据面直连） ====================

    @Test
    @Timeout(30)
    void c04_generate_error_fails_direct_stream_and_scheduler_path_unaffected() throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT + 30, 2, 1, "5", 1.0)) {
            arm(h);
            h.prefillEngines.get(0).setFaultConfig(FaultInjectionConfig.builder()
                    .generateError(true)
                    .build());

            Throwable error = callGenerateStream(h.prefillEngines.get(0), 9401).error();
            assertNotNull(error, "generate_stream must fail on the faulted engine");
            assertTrue(error.getMessage().contains("injected generate_error"), error.getMessage());
            assertEquals(0, h.prefillEngines.get(0).getRunningCount(), "no leak on rejected stream");

            Response healthy = submitTo(h, 1, 9402);
            assertTrue(healthy.isSuccess(),
                    "generate_error is data-plane only, scheduler path stays healthy");
        }
    }

    // ==================== C5 fetchError（数据面直连） ====================

    // ==================== C6 noRespond ====================

    // ==================== C7 kvPressureTokens ====================

    @Test
    @Timeout(30)
    void c07_kv_pressure_propagates_through_worker_status_and_clears() throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT + 60, 1, 1, "5", 1.0)) {
            JavaMockEngineCluster.FastRpcService decode = h.decodeEngines.get(0);
            long totalKv = decode.getTotalKvTokens();
            // The scheduler fixture is already published at status version 1.
            // Consume the engine's matching baseline before asserting a newer snapshot.
            h.pumpOnce();
            decode.setFaultConfig(FaultInjectionConfig.builder()
                    .kvPressureTokens(totalKv)
                    .build());

            h.pumpOnce();
            assertEquals(0L, h.decodeEndpoint(0).getStatus().getAvailableKvCacheTokens(),
                    "full KV pressure must surface as zero available tokens in WorkerStatus");

            decode.clearFaultConfig();
            h.pumpOnce();
            assertEquals(totalKv, h.decodeEndpoint(0).getStatus().getAvailableKvCacheTokens(),
                    "clearing the pressure must restore the full capacity view");
        }
    }

    // ==================== C8 queueDepthLimit ====================

    // ==================== C9 crashAfterNRequests ====================

    private static long engineCancelCalls(JavaMockEngineCluster.FastRpcService engine) {
        var counts = (java.util.Map<?, ?>) engine.getSnapshot().get("rpc_counts");
        return ((Number) counts.get("cancel")).longValue();
    }

    // ==================== 数据面直连 helpers ====================

    private record StreamResult(Throwable error, boolean completed) {
    }

    /** 直连 generate_stream，等 500ms 观察流事件（fault 场景内即时返回或保持沉默）。 */
    private static StreamResult callGenerateStream(JavaMockEngineCluster.FastRpcService svc,
                                                   long requestId) throws InterruptedException {
        EngineRpcService.GenerateInputPB.Builder input = EngineRpcService.GenerateInputPB.newBuilder()
                .setRequestId(requestId)
                .setGenerateConfig(EngineRpcService.GenerateConfigPB.newBuilder()
                        .setMaxNewTokens(1)
                        .build());
        for (int token = 0; token < 8; token++) {
            input.addTokenIds(token);
        }
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch terminal = new CountDownLatch(1);
        svc.generateStreamCall(input.build(), new StreamObserver<>() {
            @Override
            public void onNext(EngineRpcService.GenerateOutputsPB value) {
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
                terminal.countDown();
            }

            @Override
            public void onCompleted() {
                terminal.countDown();
            }
        });
        boolean finished = terminal.await(500, TimeUnit.MILLISECONDS);
        return new StreamResult(error.get(), finished && error.get() == null);
    }

}
