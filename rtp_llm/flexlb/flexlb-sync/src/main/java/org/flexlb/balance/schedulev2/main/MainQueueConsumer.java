package org.flexlb.balance.schedulev2.main;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.RequestPreparePool;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.SelectedPrefill;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.V2QueueReporter;
import org.flexlb.util.Logger;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * 准备池并行计算缓存匹配和请求自身耗时，主队列消费者用最新负载作最终选择。
 * 每处理一份材料，紧接着占子队列名额、抢占并交给 Prefill 队列，再处理下一份材料。
 * <p>
 * 名额上限 = min(maxQueuedRequests, Prefill 机器数 × maxInflightPerPrefillWorker × prefillSubqueueLimitMultiplier)，
 * 名额满时，高优先级请求尝试将选中 Prefill 队列里的低优先级请求退回主队列。名额保留到发布时才归还。
 * 已选中 Prefill 但拿不到子队列名额时，消费者持有请求并等待重试。
 */
final class MainQueueConsumer {

    /**
     * 没事可做时最多等这么久再扫一遍，兜底没有发出变化通知的情况。
     */
    static final long IDLE_WAIT_MS = 50L;

    private final MainQueue mainQueue;
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
     * 把请求加入选中的 Prefill 队列，返回是否入队成功。
     */
    private final Predicate<Request> addToPrefillQueue;

    MainQueueConsumer(MainQueue mainQueue,
                      RequestCleanup cleanup,
                      QueueCapacity queueCapacity,
                      EndpointRegistry endpointRegistry,
                      ConfigService configService,
                      PrefillPicker prefillPicker,
                      PrefillQueueRegistry prefillQueues,
                      RoleType prefillRole,
                      Predicate<Request> addToPrefillQueue,
                      RequestPreparePool requestPreparePool,
                      V2QueueReporter queueReporter) {
        this.queueReporter = queueReporter;
        this.mainQueue = mainQueue;
        this.cleanup = cleanup;
        this.queueCapacity = queueCapacity;
        this.endpointRegistry = endpointRegistry;
        this.configService = configService;
        this.prefillPicker = prefillPicker;
        this.prefillQueues = prefillQueues;
        this.requestPreparePool = requestPreparePool;
        this.prefillRole = prefillRole;
        this.addToPrefillQueue = addToPrefillQueue;
    }

    void run(BooleanSupplier running) {
        while (running.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
            boolean shouldWait;
            try {
                shouldWait = !runOnce(running);
            } catch (Throwable failure) {
                Logger.error("V2 main queue loop round failed", failure);
                shouldWait = true;
            }
            if (shouldWait) {
                mainQueue.awaitWakeup(IDLE_WAIT_MS);
            }
        }
    }

    /**
     * 取出一个请求处理；没有注册节点或没有请求可取时返回 false。
     */
    boolean runOnce(BooleanSupplier running) {
        if (endpointRegistry.getEndpointCount(prefillRole) == 0) {
            return false;
        }
        Request request = mainQueue.pollFirst();
        if (request == null) {
            return false;
        }
        if (request.stage() != Stage.MAIN_WAITING) {
            return true;
        }

        try {
            FlexlbConfig config = configService.loadBalanceConfig();
            String group = prefillPicker.groupOf(request.ctx());
            var estimate = request.getWorkLoadEstimate();

            if (estimate == null || estimate.expired(config.getDispatcher().getCacheMatchResultTtlMs())) {
                startPreparation(request, group);
            } else if (estimate.failure() != null) {
                request.fail(StrategyErrorType.DISPATCH_FAILED,
                        "prefill selection failed: " + estimate.failure().getMessage());
            } else {
                SelectedPrefill choice = prefillPicker.pick(request, prefillRole, group, estimate);
                sendPicked(request, choice, config, running);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            request.fail(StrategyErrorType.DISPATCH_FAILED, "main queue consumer interrupted");
        } catch (Throwable failure) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, "prefill selection failed: " + failure.getMessage());
            Logger.error("V2 prefill selection failed", failure);
        }
        return true;
    }

    private void startPreparation(Request request, String group) {
        try {
            requestPreparePool.submit(request, group);
        } catch (RejectedExecutionException rejected) {
            request.ctx().getMetrics().recordPrepareSubmitRejected();
            requestPreparePool.prepareAndAddBack(request, group);
        }
    }

    private void sendPicked(Request request, SelectedPrefill choice, FlexlbConfig config, BooleanSupplier running)
            throws InterruptedException {
        while (running.getAsBoolean() && request.stage() == Stage.MAIN_WAITING) {
            if (!reserveQueueSlot(request, choice, config)) {
                if (choice.endpoint().isRetired()) {
                    request.setWorkLoadEstimate(null);
                    mainQueue.addBackAndWake(request);
                    return;
                }
                queueReporter.blocked(request, Stage.MAIN_WAITING, QueueBlockReason.SHARED_QUEUE_FULL,
                        choice.endpoint().ipPort());
                Thread.sleep(config.getDispatcher().getSharedSlotRetryMs());
                config = configService.loadBalanceConfig();
                continue;
            }
            request.setSelectedPrefill(choice);
            if (request.moveTo(Stage.MAIN_WAITING, Stage.PREFILL_WAITING)) {
                if (!addToPrefillQueue.test(request)) {
                    request.setWorkLoadEstimate(null);
                    cleanup.returnToMain(request, Stage.PREFILL_WAITING);
                }
            } else {
                cleanup.releaseQueueSlot(request);
            }
            return;
        }
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
            // Prefill 线程实际归还名额后，等待中的消费者才能占位。
            return false;
        }
        request.holdQueueSlot(prefillRole);
        return true;
    }
}
