package org.flexlb.balance.schedulev2.prefill;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.schedulev2.LoopSignal;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.main.MainQueue;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.monitor.V2QueueReporter;
import org.flexlb.util.Logger;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 一台 Prefill 的独立队列线程。不同机器互不等待，同一机器的容量检查和 dispatch 串行执行。
 * <p>
 * 每一轮处理自己的队列：
 * <ol>
 * <li>机器已下线或同一地址已换成重新上线的机器时，把等待的请求退回主队列，丢掉这个队列；</li>
 * <li>按顺序 dispatch 等待的请求。在途满时停在队首；只有 KV 不够时，允许后面较小的请求先走，
 * 直到队首等到饥饿阈值，这台机器改为严格按顺序。</li>
 * </ol>
 * 请求先占 Prefill 在途名额，再进入 DECODE_WAITING。发布只归还排队名额，Prefill 在途名额由引擎上报回收。
 */
public final class PrefillQueueLoop {

    /**
     * 有请求在等机器空出来时，隔这么久看一次引擎有没有新的上报。
     */
    static final long BLOCKED_WAIT_MS = 5L;
    /**
     * 没有等待的请求时最多等这么久，有新请求或记录减少时会被提前唤醒。
     */
    static final long IDLE_WAIT_MS = 50L;

    private final PrefillQueueRegistry queues;
    private final PrefillQueue queue;
    private final LoopSignal loopSignal;
    private final RequestCleanup cleanup;
    private final MainQueue mainQueue;
    private final EndpointRegistry endpointRegistry;
    private final ConfigService configService;
    private final V2QueueReporter queueReporter;
    /**
     * dispatch 出去的请求交给 Decode 队列。
     */
    private final Consumer<Request> sendToDecodeQueue;
    private final AtomicBoolean running = new AtomicBoolean();
    private final Thread thread;

    public PrefillQueueLoop(PrefillQueue queue,
                            PrefillQueueRegistry queues,
                            RequestCleanup cleanup,
                            MainQueue mainQueue,
                            EndpointRegistry endpointRegistry,
                            ConfigService configService,
                            Consumer<Request> sendToDecodeQueue,
                            V2QueueReporter queueReporter) {
        this.queueReporter = queueReporter;
        this.queues = queues;
        this.queue = queue;
        this.loopSignal = queue.signal();
        this.cleanup = cleanup;
        this.mainQueue = mainQueue;
        this.endpointRegistry = endpointRegistry;
        this.configService = configService;
        this.sendToDecodeQueue = sendToDecodeQueue;
        this.thread = new Thread(this::run, "flexlb-v2-prefill-" + queue.endpoint().ipPort());
        this.thread.setDaemon(true);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            thread.start();
        }
    }

    public void close() {
        if (running.compareAndSet(true, false)) {
            loopSignal.wake();
        }
    }

    private void run() {
        while (running.get()) {
            boolean hasWaiting;
            try {
                hasWaiting = runOnce();
            } catch (Throwable failure) {
                Logger.error("V2 prefill queue loop round failed", failure);
                hasWaiting = true;
            }
            loopSignal.await(hasWaiting ? BLOCKED_WAIT_MS : IDLE_WAIT_MS);
        }
    }

    /**
     * 处理自己这台机器的队列，返回还有没有请求在等 dispatch。机器下线时退回请求并停止这个线程。
     */
    boolean runOnce() {
        int maxInflight = configService.loadBalanceConfig().getDispatcher().getMaxInflightPerPrefillWorker();
        returnMarkedRequests();
        if (!isCurrent(queue)) {
            queues.remove(queue);
            sendBackToMainQueue(queue);
            close();
            return false;
        }
        dispatchWaiting(queue, maxInflight);
        return queue.waitingCount() > 0;
    }

    /**
     * 退回处理不受 Prefill 在途上限限制，由同一个线程串行处理退回和下发。
     */
    private void returnMarkedRequests() {
        Request request;
        while ((request = queue.pollPendingReturn()) != null) {
            returnToMain(request);
        }
    }

    private void returnToMain(Request request) {
        if (!request.moveTo(Stage.PREFILL_WAITING, Stage.MAIN_WAITING)) {
            return;
        }
        Integer preemptorPriority = request.queueProgress().prefillReturnPriority();
        queue.removeWaiting(request);
        request.setSelectedPrefill(null);
        request.setPrefillDecidedAtMs(0L);
        cleanup.releaseQueueSlot(request);
        if (preemptorPriority != null) {
            queueReporter.preemptedReturn(preemptorPriority, request.priority(), queue.endpoint().ipPort());
        }
        mainQueue.addBackAndWake(request);
    }

    /**
     * 队列对应的机器是否还在：注册表里这个地址还是同一个 PrefillEndpoint 对象。
     */
    private boolean isCurrent(PrefillQueue queue) {
        PrefillEndpoint endpoint = queue.endpoint();
        return !endpoint.isRetired()
                && endpointRegistry.get(endpoint.getStatus().getRole(), endpoint.ipPort()) == endpoint;
    }

    private void dispatchWaiting(PrefillQueue queue, int maxInflight) {
        Iterator<Request> iterator = queue.waitingIterator();
        while (iterator.hasNext()) {
            Request request = iterator.next();
            if (request.stage() != Stage.PREFILL_WAITING) {
                queue.removeWaiting(request);
                continue;
            }
            if (request.queueProgress().prefillReturnPriority() != null) {
                continue;
            }
            QueueBlockReason reason = tryDispatch(queue, request, maxInflight);
            if (reason == QueueBlockReason.PREFILL_INFLIGHT_FULL) {
                queueReporter.blocked(request, Stage.PREFILL_WAITING, reason, queue.endpoint().ipPort());
            }
            if (reason == QueueBlockReason.PREFILL_INFLIGHT_FULL || reason == QueueBlockReason.ENDPOINT_RETIRED) {
                return;
            }
        }
    }

    private QueueBlockReason tryDispatch(PrefillQueue queue, Request request, int maxInflight) {
        QueueBlockReason reason = queue.tryReserveForDispatch(request, maxInflight, configService);
        if (reason != null) {
            return reason;
        }
        if (!isCurrent(queue)) {
            queue.removeDispatched(request);
            return QueueBlockReason.ENDPOINT_RETIRED;
        }
        if (!request.moveTo(Stage.PREFILL_WAITING, Stage.DECODE_WAITING)) {
            queue.removeDispatched(request);
            if (request.stage() != Stage.PREFILL_WAITING) {
                queue.removeWaiting(request);
            }
            return QueueBlockReason.RESERVATION_CHANGED;
        }
        queue.removeWaiting(request);
        request.setPrefillDecidedAtMs(System.currentTimeMillis());
        try {
            sendToDecodeQueue.accept(request);
        } catch (RuntimeException failure) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, "prefill handoff failed: " + failure.getMessage());
            throw failure;
        }
        return null;
    }

    /**
     * 机器下线后，把还在等待的请求退回主队列重新选 Prefill。已 dispatch 的请求不受影响。
     */
    private void sendBackToMainQueue(PrefillQueue queue) {
        Iterator<Request> iterator = queue.waitingIterator();
        while (iterator.hasNext()) {
            Request request = iterator.next();
            returnToMain(request);
        }
    }
}
