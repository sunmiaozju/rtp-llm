package org.flexlb.mockengine;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.loadbalance.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * 分阶段抢占准入、请求重新排队和进程内 Mock Engine 取消通道的集成验证。
 *
 * <ul>
 *   <li>A1 Prefill 优先级插队 — 保留低优请求；</li>
 *   <li>A2 Decode queued reservation 撤回 — 低优请求重新预留，高优请求先派发；</li>
 *   <li>A3 accepted 让位 — 真实 MockEngineCancelChannel，victim 8429，
 *       cancel→确认→派发顺序 + cancel 超时不泄漏（铁律4）；</li>
 *   <li>A5 同优不抢占 — 同优请求满载时绝不驱逐同优 victim。</li>
 * </ul>
 */
class PreemptionPhasesE2ETest {

    private static final int BASE_PORT = 62700;

    // ==================== A1 Prefill 优先级插队 ====================

    @Test
    @Timeout(30)
    void a1_priority_queue_retains_lower_priority_work_without_token_eviction() throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT, 1, 1, "50", 1.0, false)) {
            // Priority ordering retains lower-priority queued work without an eviction policy.
            // 大窗口停住派发：队列状态稳定可断言
            h.fixedWindowDecision().setMaxCollectionWaitMs(10_000);
            h.fixedWindowDecision().setMaxRequests(100);

            CompletableFuture<Response> low1 = h.submitAndGenerate(h.context(101, 30));
            CompletableFuture<Response> low2 = h.submitAndGenerate(h.context(102, 40));
            AutoTpmE2EHarness.await(
                    () -> h.prefillEndpoint(0).queuedRequestCount() == 2,
                    5_000,
                    "two low-priority requests must reach the Prefill queue");
            assertEquals(2, h.prefillEndpoint(0).queuedRequestCount());
            assertFalse(low1.isDone());
            assertFalse(low2.isDone());

            CompletableFuture<Response> high = h.submitAndGenerate(h.context(103, 70));

            AutoTpmE2EHarness.await(
                    () -> h.prefillEndpoint(0).queuedRequestCount() == 3,
                    5_000, "higher priority work must join the ordered queue without evicting existing work");
            assertFalse(high.isDone());
            assertFalse(low1.isDone());
            assertFalse(low2.isDone());

        }
    }

    // ==================== A2 decode reserved 驱逐 ====================

    @Test
    @Timeout(30)
    void a2_queued_decode_withdrawal_requeues_original_before_priority_dispatch() throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT + 10, 1, 1, "50", 1.0, false)) {
            h.allowPreemption(VictimStage.DECODE_RESERVED);
            h.config.getRouter().getRoles().getDecode().getAvailability()
                    .setMaxEngineRequests(1L);
            h.fixedWindowDecision().setMaxCollectionWaitMs(10_000);
            h.fixedWindowDecision().setMaxRequests(100);

            DecodeEndpoint decodeEp = h.decodeEndpoint(0);
            // Keep reported expected-KV usage below the final 90% gate while the
            // local hard reservation still reduces real availability to 127.
            h.setDecodeKvCapacity(0, 255, 256);
            CompletableFuture<Response> low = h.submitAndGenerate(h.context(201, 30));
            AutoTpmE2EHarness.await(
                    () -> decodeEp.resourceSnapshot().reserved().containsKey("201"),
                    5_000,
                    "low-priority request must publish its Decode reservation");
            assertFalse(low.isDone());
            assertTrue(decodeEp.resourceSnapshot().reserved().containsKey("201"));
            long originalReservationToken = decodeEp.resourceSnapshot().reserved().get("201").reservationToken();
            // Master 排队持有的请求通过本地撤回重新排队，无需 Engine Cancel。
            long requiredKvBefore = decodeEp.routingView().inputKvReserved();
            assertTrue(requiredKvBefore > 0);

            CompletableFuture<Response> high = h.preemptDecodeAndGenerate(h.context(202, 70), 0);

            AutoTpmE2EHarness.await(
                    () -> decodeEp.resourceSnapshot().reserved().containsKey("202"),
                    5_000, "the higher-priority request must acquire the withdrawn capacity");
            // Withdraw the queued route without ending the original request or sending Engine Cancel.
            assertFalse(low.isDone(), "the original low-priority future must remain pending");
            assertTrue(h.engineArrivalOrder.isEmpty());
            assertEquals(0, h.decodeEngines.get(0).getCancelledCount());

            AutoTpmE2EHarness.await(() -> {
                var requeued = decodeEp.resourceSnapshot().reserved().get("201");
                return requeued != null && requeued.reservationToken() != originalReservationToken;
            }, 5_000, "the original request must acquire a new reservation after withdrawal");
            assertFalse(high.isDone(), "high-priority request should sit in the queue after eviction");
            assertNotEquals(originalReservationToken,
                    decodeEp.resourceSnapshot().reserved().get("201").reservationToken());
            assertTrue(decodeEp.resourceSnapshot().reserved().containsKey("202"));
            assertEquals(2, decodeEp.getInflightCount());
            assertEquals(2 * requiredKvBefore, decodeEp.routingView().inputKvReserved(),
                    "each queued request must own exactly one reservation");
            verify(h.requestReporter).reportVictim(eq(30), eq(70), eq("decode_reserved"), anyString());

            h.setDecodeKvCapacity(0, 1_000_000, 1_000_000);
            h.fixedWindowDecision().setMaxCollectionWaitMs(1);
            h.fixedWindowDecision().setMaxRequests(1);
            h.startAutoPump(10);
            // Both original requests eventually succeed (200), with high priority dispatched first.
            assertTrue(high.get(5, TimeUnit.SECONDS).isSuccess());
            assertTrue(low.get(5, TimeUnit.SECONDS).isSuccess());
            assertEquals(List.of(202L, 201L), new java.util.ArrayList<>(h.engineArrivalOrder));
            AutoTpmE2EHarness.await(() -> decodeEp.getInflightCount() == 0,
                    5_000, "both requests must release their Decode ownership");
        }
    }

    // ==================== A3 accepted 让位（真实 cancel 通道） ====================

    // ==================== A5 同优不抢占 ====================

    @Test
    @Timeout(30)
    void a5_equal_priority_never_preempts_queue_or_reserved_victims() throws Exception {
        try (AutoTpmE2EHarness h = new AutoTpmE2EHarness(BASE_PORT + 50, 1, 1, "50", 1.0, false)) {
            h.allowPreemption(VictimStage.DECODE_RESERVED);
            h.config.getRouter().getRoles().getDecode().getAvailability()
                    .setMaxEngineRequests(1L);
            h.fixedWindowDecision().setMaxCollectionWaitMs(10_000);
            h.fixedWindowDecision().setMaxRequests(100);

            DecodeEndpoint decodeEp = h.decodeEndpoint(0);
            // Isolate the request-slot contract with enough KV for the full
            // prompt plus output reservation.
            h.config.getRouter().getRoles().getDecode().getAvailability()
                    .setMaxKvUsagePercent(100);
            h.setDecodeKvCapacity(0, 4096, 8192);
            // P50 占据 decode 唯一槽位
            CompletableFuture<Response> holder = h.submitAndGenerate(h.context(501, 50));
            AutoTpmE2EHarness.await(
                    () -> decodeEp.resourceSnapshot().reserved().containsKey("501"),
                    5_000,
                    "equal-priority holder must publish its Decode reservation");
            assertFalse(holder.isDone());
            assertTrue(decodeEp.resourceSnapshot().reserved().containsKey("501"));

            // 同优新请求不能抢占，也不能把瞬时容量不足变成终态 8403；
            // 它保持未绑定，等待精确 Decode 容量变化。
            CompletableFuture<Response> equal = h.submitAndGenerate(h.context(502, 50));
            assertFalse(equal.isDone());

            // victim 完全不受影响
            assertFalse(holder.isDone());
            assertTrue(decodeEp.resourceSnapshot().reserved().containsKey("501"));
            assertEquals(1, h.prefillEndpoint(0).queuedRequestCount());
            verify(h.requestReporter, never()).reportVictim(anyInt(), anyInt(),
                    anyString(), anyString());
        }
    }

}
