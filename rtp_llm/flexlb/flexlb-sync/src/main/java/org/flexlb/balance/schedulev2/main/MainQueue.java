package org.flexlb.balance.schedulev2.main;

import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.scheduler.SchedulerErrorMessages;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestOrder;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.monitor.V2QueueReporter;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 主队列：还没选 Prefill 的请求在这里排队。
 * <p>
 * 开启优先级排序时按优先级从高到低、同优先级先进先出；否则只按先进先出。
 * 入队时检查排队总数（主队列、Prefill 队列和 Decode 队列里尚未发布的请求），
 * 总数用 {@link QueueCapacity} 记账，请求发布、失败或取消时减掉。
 */
public final class MainQueue {

    private final V2QueueReporter reporter;
    private final QueueCapacity queueCapacity;
    private final Semaphore pendingWakeups = new Semaphore(0);
    private final int maxPendingWakeups;
    private final ConcurrentSkipListSet<Request> requests;
    private final AtomicLong nextSeq = new AtomicLong();

    public MainQueue(QueueCapacity queueCapacity,
                     int consumerThreads,
                     boolean priorityOrdering,
                     V2QueueReporter reporter) {
        this.reporter = reporter;
        this.queueCapacity = queueCapacity;
        this.maxPendingWakeups = consumerThreads;
        this.requests = new ConcurrentSkipListSet<>(RequestOrder.of(priorityOrdering));
    }

    /**
     * 请求入队。排队总数已满时：
     * <ul>
     * <li>没开优先级排序，返回 QUEUE_FULL；</li>
     * <li>请求优先级低于高优先级阈值，返回 QUEUE_FULL_LOW_PRIORITY_REJECTED；</li>
     * <li>否则尝试挤掉主队列里一个更低优先级的请求（最低优先级里最晚入队的），有剩余次数才重新申请名额。
     * 找不到可挤的请求或用完尝试次数时返回 QUEUE_FULL_HIGH_PRIORITY_REJECTED。</li>
     * </ul>
     * 入队成功返回 null。
     */
    public StrategyErrorType add(Request request, FlexlbConfig config) {
        long limit = config.queueScheduler().getMaxQueuedRequests();
        int maxAddAttempts = config.queueScheduler().getMainQueueMaxAddAttempts();
        for (int attempt = 0; attempt < maxAddAttempts; attempt++) {
            if (queueCapacity.tryIncreaseTotalQueueCount(request.future(), limit)) {
                request.setQueueSeq(nextSeq.incrementAndGet());
                requests.add(request);
                // 入队期间请求可能已结束，结束时的清理没看到它，这里补上
                if (request.stage().isEnded()) {
                    remove(request);
                    queueCapacity.decreaseTotalQueueCount(request.future());
                }
                wakeConsumer();
                return null;
            }
            if (!config.isPriorityOrdering()) {
                return StrategyErrorType.QUEUE_FULL;
            }
            int threshold = config.priorityOrdering().getHighPriorityThreshold();
            if (request.priority() < threshold) {
                return StrategyErrorType.QUEUE_FULL_LOW_PRIORITY_REJECTED;
            }
            Request victim = findLowerPriorityRequest(request.priority());
            if (victim == null) {
                return StrategyErrorType.QUEUE_FULL_HIGH_PRIORITY_REJECTED;
            }
            // 先从主队列移除，不能淘汰已被消费者取走的请求。
            if (requests.remove(victim) && victim.fail(Stage.MAIN_WAITING,
                    StrategyErrorType.queueRejectionFor(victim.priority(), threshold),
                    SchedulerErrorMessages.QUEUE_FULL)) {
                reporter.mainPreempted(request.priority(), victim.priority());
            }
        }
        return StrategyErrorType.QUEUE_FULL_HIGH_PRIORITY_REJECTED;
    }

    private Request findLowerPriorityRequest(int priority) {
        for (Request queued : requests.descendingSet()) {
            if (queued.priority() >= priority) {
                return null;
            }
            if (queued.stage() == Stage.MAIN_WAITING) {
                return queued;
            }
        }
        return null;
    }

    Request pollFirst() {
        return requests.pollFirst();
    }

    public void addBackAndWake(Request request) {
        addBack(request);
        wakeConsumer();
    }

    void wakeConsumer() {
        // 通知不是排队名额，并发发通知时允许短暂超过软上限。
        if (pendingWakeups.availablePermits() < maxPendingWakeups) {
            pendingWakeups.release();
        }
    }

    void awaitWakeup(long timeoutMs) {
        try {
            pendingWakeups.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 按原顺序号放回。请求仍占用总排队名额，不重新检查上限。
     */
    void addBack(Request request) {
        if (request.stage().isEnded()) {
            return;
        }
        requests.add(request);
        // 取消或失败的清理可能先于插入，这里补删。
        if (request.stage().isEnded()) {
            remove(request);
        }
    }

    /**
     * 只从主队列里拿掉这个请求，不改阶段，也不动排队总数和名额。
     */
    public void remove(Request request) {
        requests.remove(request);
    }

    public int size() {
        return requests.size();
    }
}
