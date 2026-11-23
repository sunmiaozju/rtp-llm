package org.flexlb.balance.schedulev2;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.eviction.DecodePreemptionCoordinator;
import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.scheduler.RequestState;
import org.flexlb.balance.scheduler.SchedulerErrorMessages;
import org.flexlb.balance.schedulev2.decode.DecodeQueue;
import org.flexlb.balance.schedulev2.decode.DecodeQueueConsumer;
import org.flexlb.balance.schedulev2.decode.DecodeQueueConsumerPoll;
import org.flexlb.balance.schedulev2.main.MainQueue;
import org.flexlb.balance.schedulev2.main.MainQueueConsumerPool;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueLoop;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestTable;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.config.ConfigService;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.V2QueueReporter;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NON_BATCH_V2 链路的入口，只处理 Generation 请求，Encoder 请求仍由 RequestScheduler 处理。
 */
@Lazy
@Component
public class RequestSchedulerV2 {

    private final ConfigService configService;
    private final Set<RoleType> deployedRoles;
    /**
     * Prefill 用的角色：PD 分离时是 PREFILL，PD 合一时是 PDFUSION；两者都没部署时为 null。
     */
    private final RoleType prefillRole;
    private final RequestTable requestTable = new RequestTable();
    private final MainQueue mainQueue;
    private final PrefillQueueRegistry prefillQueues;
    private final DecodeQueue decodeQueue;
    private final RequestCleanup cleanup;
    private final PrefillPublishCheck prefillCheck;
    private final MainQueueConsumerPool mainQueueConsumerPool;
    private final DecodeQueueConsumerPoll decodeQueueConsumerPoll;
    private final ScheduledThreadPoolExecutor deadlineTimer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final V2QueueReporter queueReporter;
    private final EndpointRegistry endpointRegistry;

    public RequestSchedulerV2(ConfigService configService,
                              QueueCapacity queueCapacity,
                              EndpointRegistry endpointRegistry,
                              PlacementAvailability placementAvailability,
                              DefaultRouter router,
                              CostBasedPrefillStrategy prefillStrategy,
                              DecodeSelector decodeSelector,
                              PrefillQueueRegistry prefillQueues,
                              ModelMetaConfig modelMetaConfig,
                              DecodePreemptionCoordinator preemptionCoordinator,
                              V2QueueReporter queueReporter) {
        this.queueReporter = queueReporter;
        this.endpointRegistry = endpointRegistry;
        this.configService = configService;
        this.prefillCheck = new PrefillPublishCheck(endpointRegistry, prefillQueues);
        this.deployedRoles = modelMetaConfig.getRequiredRoles();
        this.prefillRole = deployedRoles.contains(RoleType.PREFILL)
                ? RoleType.PREFILL
                : deployedRoles.contains(RoleType.PDFUSION) ? RoleType.PDFUSION : null;
        var startupConfig = configService.loadBalanceConfig();
        var dispatcher = startupConfig.getDispatcher();
        boolean priorityOrdering = startupConfig.isPriorityOrdering();
        this.mainQueue =
                new MainQueue(queueCapacity, dispatcher.getMainQueueConsumerThreads(), priorityOrdering, queueReporter);
        this.prefillQueues = prefillQueues;
        this.decodeQueue = new DecodeQueue(priorityOrdering);
        this.cleanup = new RequestCleanup(requestTable, mainQueue, queueCapacity, prefillQueues, decodeQueue);
        this.mainQueueConsumerPool = new MainQueueConsumerPool(mainQueue, dispatcher, cleanup, queueCapacity,
                endpointRegistry, placementAvailability, configService, router, prefillStrategy, prefillQueues,
                prefillRole, queueReporter);
        Runnable capacityWake = decodeQueue::wake;
        this.decodeQueueConsumerPoll =
                new DecodeQueueConsumerPoll(configService.loadBalanceConfig().getDispatcher().getDecodeQueueThreads(),
                        decodeQueue, placementAvailability, () -> new DecodeQueueConsumer(decodeQueue, cleanup,
                                decodeSelector, preemptionCoordinator, prefillCheck, queueReporter, capacityWake));
        this.deadlineTimer = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "flexlb-v2-deadline");
            thread.setDaemon(true);
            return thread;
        });
        this.deadlineTimer.setRemoveOnCancelPolicy(true);
        prefillQueues.start(queue -> {
            queue.onCapacityChanged(decodeQueue::wake);
            return new PrefillQueueLoop(queue, prefillQueues, cleanup, mainQueue, endpointRegistry, configService,
                    this::sendToDecodeQueue, queueReporter);
        });
        mainQueueConsumerPool.start();
        decodeQueueConsumerPoll.start();
    }

    public CompletableFuture<Response> submit(BalanceContext context) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(
                    Response.buildErrorResponse(StrategyErrorType.DISPATCH_FAILED,
                            SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN));
        }
        if (prefillRole == null || !rolesAreDeployed(context)) {
            return CompletableFuture.completedFuture(
                    Response.buildErrorResponse(StrategyErrorType.INVALID_REQUEST, "requested roles are not deployed"));
        }
        long nowMs = System.currentTimeMillis();
        if (context.requestExpired(nowMs)) {
            return CompletableFuture.completedFuture(Response.buildErrorResponse(
                    StrategyErrorType.queueRejectionFor(context.getPriority(),
                            context.getConfig().queueScheduler().getOrdering().getHighPriorityThreshold()),
                    SchedulerErrorMessages.DEADLINE_EXPIRED_BEFORE_PLACEMENT));
        }
        CompletableFuture<Response> future = new CompletableFuture<>();
        Request request = new Request(context, future, nowMs);
        if (!requestTable.add(request)) {
            return CompletableFuture.completedFuture(Response.buildErrorResponse(StrategyErrorType.INVALID_REQUEST,
                    SchedulerErrorMessages.duplicateRequestId(context.getRequestId())));
        }
        context.setEnqueueTime(nowMs);
        context.setFuture(future);
        future.whenComplete((response, failure) -> {
            handleFutureDone(request, failure);
            queueReporter.completed(request, response);
        });
        long delayMs = Math.max(0L, context.getRequestExpiresAtMs() - nowMs);
        try {
            request.setDeadlineTask(deadlineTimer.schedule(() -> request.cancel(CancelReason.DEADLINE_EXCEEDED),
                    delayMs, TimeUnit.MILLISECONDS));
        } catch (RejectedExecutionException closing) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN);
            return future;
        }
        StrategyErrorType rejection = mainQueue.add(request, configService.loadBalanceConfig());
        if (rejection != null) {
            request.fail(rejection, SchedulerErrorMessages.QUEUE_FULL);
            return future;
        }
        if (closed.get()) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN);
        }
        return future;
    }

    /**
     * 请求指定的角色（为空表示全部）都在这个部署里。
     */
    private boolean rolesAreDeployed(BalanceContext context) {
        Set<RoleType> requested = context.getRequestedRoles();
        return requested == null || deployedRoles.containsAll(requested);
    }

    /**
     * Prefill dispatch 出去的请求：要 Decode 时进 Decode 队列，不要 Decode（PD 合一）时直接发布 Prefill 的地址。
     */
    private void sendToDecodeQueue(Request request) {
        if (!needsDecode(request)) {
            if (!prefillCheck.isAvailable(request)) {
                cleanup.returnToMain(request, Stage.DECODE_WAITING);
                return;
            }
            try {
                if (request.publish(Stage.DECODE_WAITING, List.of(request.selectedPrefill().serverStatus()))) {
                    cleanup.releasePublished(request);
                }
            } finally {
                if (request.stage() != Stage.PUBLISHED) {
                    prefillCheck.release(request);
                }
            }
            return;
        }
        decodeQueue.addWaiting(request);
    }

    /**
     * 部署了 Decode，并且请求指定的角色（为空表示全部）里有 Decode。
     */
    private boolean needsDecode(Request request) {
        Set<RoleType> requested = request.ctx().getRequestedRoles();
        return deployedRoles.contains(RoleType.DECODE) && (requested == null || requested.contains(RoleType.DECODE));
    }

    /**
     * future 完成后的收尾：外部取消 future 时把请求标为取消；请求失败或取消后归还它占的名额和计数。
     */
    private void handleFutureDone(Request request, Throwable failure) {
        if (failure instanceof CancellationException) {
            request.cancel(CancelReason.CLIENT_CANCELLED);
        }
        if (request.stage().isEnded()) {
            cleanup.releaseEnded(request);
        }
    }

    public RequestState cancelRequest(String requestId, long expectedBatchId, CancelReason reason) {
        Request request = requestTable.get(requestId);
        if (request == null || !request.toRequestState().matchesBatch(expectedBatchId)) {
            return null;
        }
        request.cancel(reason);
        return request.toRequestState();
    }

    public RequestState getRequestState(String requestId, long expectedBatchId) {
        Request request = requestTable.get(requestId);
        if (request == null) {
            return null;
        }
        RequestState state = request.toRequestState();
        return state.matchesBatch(expectedBatchId) ? state : null;
    }

    public int trackedRequestCount() {
        return requestTable.size();
    }

    @Scheduled(fixedDelay = 20000L)
    public void reportQueues() {
        List<String> endpoints = prefillRole == null
                ? List.of()
                : endpointRegistry.prefillRoutingSnapshot(prefillRole)
                        .stream()
                        .map(entry -> entry.endpoint().ipPort())
                        .toList();
        queueReporter.sample(requestTable, endpoints);
        queueReporter.samplePrefillInflight(prefillQueues.list(),
                configService.loadBalanceConfig().getDispatcher().getMaxInflightPerPrefillWorker());
    }

    public int queuedRequestCount() {
        return requestTable.waitingCount();
    }

    public int blockedRequestCount() {
        return 0;
    }

    public List<RequestState> snapshotActiveRequests() {
        List<RequestState> states = new ArrayList<>();
        for (Request request : requestTable.list()) {
            states.add(request.toRequestState());
        }
        states.sort(Comparator.comparingLong(RequestState::createdAtMs).thenComparing(RequestState::requestId));
        return states;
    }

    /**
     * 停止接收新请求，并让所有还在等待的请求失败。
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // 1. 先结束请求并通知消费者，再停止线程，消费者退出前归还未发布的两端资源。
        for (Request request : requestTable.list()) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN);
        }
        mainQueueConsumerPool.close();
        prefillQueues.close();
        decodeQueueConsumerPoll.close();
        deadlineTimer.shutdownNow();
    }
}
