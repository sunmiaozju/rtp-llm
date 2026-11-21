package org.flexlb.balance.schedulev2.main;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.schedulev2.LoopSignal;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.RequestPreparePool;
import org.flexlb.balance.schedulev2.SelectionResult;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.SelectedPrefill;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.PrefillInputs;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.V2QueueReporter;
import org.flexlb.util.Logger;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * 准备池并行计算缓存匹配和请求自身耗时，主队列消费者用最新负载作最终选择。
 * 每处理一份材料，紧接着占子队列名额、抢占并交给 Prefill 队列，再处理下一份材料。
 * <p>
 * 名额上限 = min(maxQueuedRequests, Prefill 机器数 × maxInflightPerPrefillWorker × prefillSubqueueLimitMultiplier)，
 * 名额满时，高优先级请求尝试将选中 Prefill 队列里的低优先级请求退回主队列。名额保留到发布时才归还。
 * 请求暂时没有可选的 Prefill 或拿不到名额时，留在主队列，继续检查后续请求。
 */
final class MainQueueConsumer {

    /**
     * 没事可做时最多等这么久再扫一遍，兜底没有发出变化通知的情况。
     */
    static final long IDLE_WAIT_MS = 50L;
    /**
     * 每轮最多检查这么多个仍在主队列等待的请求。
     */
    static final int MAX_SCAN_PER_ROUND = 256;

    private final MainQueue mainQueue;
    private final LoopSignal loopSignal;
    private final RequestCleanup cleanup;
    private final QueueCapacity queueCapacity;
    private final EndpointRegistry endpointRegistry;
    private final ConfigService configService;
    private final PrefillPicker prefillPicker;
    private final PrefillQueueRegistry prefillQueues;
    private final RequestPreparePool requestPreparePool;
    private final V2QueueReporter queueReporter;
    /**
     * 这个部署里 Prefill 用的角色：PD 分离时是 PREFILL，PD 合一时是 PDFUSION。
     */
    private final RoleType prefillRole;
    /**
     * 选好 Prefill 后把请求交给 Prefill 队列，请求上已经带着选好的 {@link SelectedPrefill}。
     */
    private final Consumer<Request> sendToPrefillQueue;

    MainQueueConsumer(MainQueue mainQueue,
                      LoopSignal loopSignal,
                      RequestCleanup cleanup,
                      QueueCapacity queueCapacity,
                      EndpointRegistry endpointRegistry,
                      ConfigService configService,
                      PrefillPicker prefillPicker,
                      PrefillQueueRegistry prefillQueues,
                      RoleType prefillRole,
                      Consumer<Request> sendToPrefillQueue,
                      RequestPreparePool requestPreparePool,
                      V2QueueReporter queueReporter) {
        this.queueReporter = queueReporter;
        this.mainQueue = mainQueue;
        this.loopSignal = loopSignal;
        this.cleanup = cleanup;
        this.queueCapacity = queueCapacity;
        this.endpointRegistry = endpointRegistry;
        this.configService = configService;
        this.prefillPicker = prefillPicker;
        this.prefillQueues = prefillQueues;
        this.requestPreparePool = requestPreparePool;
        this.prefillRole = prefillRole;
        this.sendToPrefillQueue = sendToPrefillQueue;
    }

    void run(BooleanSupplier running) {
        while (running.getAsBoolean()) {
            boolean moved;
            try {
                moved = runOnce();
            } catch (Throwable failure) {
                Logger.error("V2 main queue loop round failed", failure);
                moved = false;
            }
            if (!moved) {
                loopSignal.await(IDLE_WAIT_MS);
            }
        }
    }

    /**
     * 扫一轮主队列，返回这一轮有没有请求离开主队列。
     */
    boolean runOnce() {
        if (endpointRegistry.getEndpointCount(prefillRole) == 0) {
            return false;
        }
        FlexlbConfig config = configService.loadBalanceConfig();
        boolean moved = false;
        int scanned = 0;
        Iterator<Request> iterator = mainQueue.iterator();
        while (iterator.hasNext() && scanned < MAX_SCAN_PER_ROUND) {
            Request request = iterator.next();
            if (request.stage() != Stage.MAIN_WAITING) {
                mainQueue.remove(request);
                continue;
            }
            String group = prefillPicker.groupOf(request.ctx());
            scanned++;
            CompletableFuture<SelectionResult<PrefillInputs>> estimateFuture = request.getWorkLoadEstimate();
            SelectionResult<PrefillInputs> estimatedCompleted =
                    estimateFuture == null ? null : estimateFuture.getNow(null);
            // 已经在执行中，但是未完成
            if (estimateFuture != null && estimatedCompleted == null) {
                continue;
            }
            // 未开始执行或结果已过期
            if (estimatedCompleted == null
                    || estimatedCompleted.expired(config.getDispatcher().getCacheMatchResultTtlMs())) {
                request.setWorkLoadEstimate(null);
                requestPreparePool.submit(request, () -> prefillPicker.prepare(request.ctx(), prefillRole, group));
                continue;
            }
            try {
                if (request.stage() != Stage.MAIN_WAITING) {
                    continue;
                }
                if (estimatedCompleted.failure() != null) {
                    request.fail(StrategyErrorType.DISPATCH_FAILED,
                            "prefill selection failed: " + estimatedCompleted.failure().getMessage());
                    moved = true;
                } else if (!prefillPicker.inputsAreCurrent(estimatedCompleted.value(), prefillRole, group)) {
                    queueReporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.CANDIDATES_CHANGED, "all");
                    request.setWorkLoadEstimate(null);
                } else if (pickPrefill(request, group, estimatedCompleted.value(), config)) {
                    moved = true;
                }
            } catch (Throwable failure) {
                request.fail(StrategyErrorType.DISPATCH_FAILED, "prefill selection failed: " + failure.getMessage());
                Logger.error("V2 prefill selection result failed", failure);
                moved = true;
            }
        }
        return moved;
    }

    /**
     * 选 Prefill 并尝试占子队列名额。返回 false 表示暂时没有机器或名额，请求留在主队列。
     */
    private boolean pickPrefill(Request request, String group, PrefillInputs inputs, FlexlbConfig config) {
        PlacementResult<SelectedRole, RoleType> result = prefillPicker.pick(request, prefillRole, group, inputs);
        switch (result.status()) {
            case SUCCESS -> {
                try (SelectedRole selected = result.value()) {
                    return sendPicked(request, selected, config);
                }
            }
            case BLOCKED -> {
                queueReporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.NO_AVAILABLE_PREFILL, "all");
                return false;
            }
            default -> {
                if (result.failure() == null) {
                    request.fail(StrategyErrorType.DISPATCH_FAILED, "prefill selection " + result.status());
                } else {
                    request.fail(result.failure());
                }
                return true;
            }
        }
    }

    private boolean sendPicked(Request request, SelectedRole selected, FlexlbConfig config) {
        SelectedPrefill choice;
        try {
            choice = SelectedPrefill.from(selected, request.seqLen());
        } catch (RuntimeException failure) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, "prefill selection failed: " + failure.getMessage());
            return true;
        }
        if (request.stage() != Stage.MAIN_WAITING) {
            return true;
        }
        if (!reserveQueueSlot(request, choice, config)) {
            queueReporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL,
                    choice.endpoint().ipPort());
            return false;
        }
        request.setSelectedPrefill(choice);
        if (!request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING)) {
            // 选的过程中请求已经结束
            cleanup.releaseQueueSlot(request);
            return true;
        }
        mainQueue.remove(request);
        sendToPrefillQueue.accept(request);
        return true;
    }

    /**
     * 先尝试占共享名额；满了只标记选中 Prefill 队列里的一个低优先级请求，等待 Prefill 线程退回并归还名额。
     */
    private boolean reserveQueueSlot(Request request, SelectedPrefill choice, FlexlbConfig config) {
        int endpointCount = endpointRegistry.getEndpointCount(prefillRole);
        if (!queueCapacity.tryReserveEngineSubQueueSlot(config, prefillRole, endpointCount)) {
            if (!config.isPriorityOrdering()
                    || request.priority() < config.priorityOrdering().getHighPriorityThreshold()) {
                return false;
            }
            PrefillQueue queue = prefillQueues.find(choice.endpoint());
            if (queue != null) {
                queue.markLowerPriorityForReturn(request.priority());
            }
            // Prefill 线程完成退回并归还名额后会唤醒主队列，本轮不提前占用尚未释放的名额。
            return false;
        }
        request.holdQueueSlot(prefillRole);
        return true;
    }
}
