package org.flexlb.balance.scheduler;

import org.flexlb.balance.schedulev2.RequestSchedulerV2;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RequestPhase;
import org.flexlb.dao.route.RoleType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Sends Generation requests to V2 and keeps the separate Encoder request lifecycle.
 */
@Component
public final class RequestScheduler {

    private final DefaultRouter router;
    private final RequestRegistry requestRegistry;
    private final RequestSchedulerV2 schedulerV2;
    private final EncoderQueueCoordinator encoderQueue;

    RequestScheduler(ConfigService configService,
                     DefaultRouter router,
                     RequestRegistry requestRegistry,
                     RequestSchedulerV2 schedulerV2) {
        this.router = router;
        this.requestRegistry = requestRegistry;
        this.schedulerV2 = schedulerV2;
        this.encoderQueue = router.hasEncoderRole()
                ? new EncoderQueueCoordinator(router, requestRegistry, configService.loadBalanceConfig().isPriorityOrdering())
                : null;
    }

    /**
     * Register once, then enter the Encoder or Generation ordered queue.
     */
    public CompletableFuture<Response> submit(BalanceContext context) {
        if (context == null || context.getRequest() == null) {
            return CompletableFuture.completedFuture(error(
                    StrategyErrorType.INVALID_REQUEST, null));
        }
        Set<RoleType> requestedRoles = context.getRequestedRoles();
        if (requestedRoles != null && requestedRoles.contains(RoleType.ENCODER)
                && !router.hasEncoderRole()) {
            return CompletableFuture.completedFuture(error(StrategyErrorType.INVALID_REQUEST,
                    "Encoder role is not configured for this model"));
        }
        if (!router.isEncoderOnly(context)) {
            return schedulerV2.submit(context);
        }
        context.setRequestPhase(RequestPhase.ENCODER);
        CompletableFuture<Response> future = requestRegistry.register(context);
        context.setFuture(future);
        if (!future.isDone()) {
            enqueueEncoderRequest(context, future);
        }
        return future;
    }

    private void enqueueEncoderRequest(BalanceContext context, CompletableFuture<Response> future) {
        try {
            if (!encoderQueue.offer(context, future)) {
                future.complete(error(StrategyErrorType.DISPATCH_FAILED,
                        SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN));
            }
        } catch (RuntimeException failure) {
            future.complete(error(StrategyErrorType.DISPATCH_FAILED,
                    "Encoder queue submission failed: " + failure.getMessage()));
        }
    }

    public RequestState cancelRequest(String requestId, long expectedBatchId, CancelReason reason) {
        return schedulerV2.cancelRequest(requestId, expectedBatchId, reason);
    }

    /**
     * Cancel the selected phase while preserving the other phase with the same request ID.
     */
    public RequestState cancelRequest(String requestId, long expectedBatchId, CancelReason reason, RequestPhase phase) {
        if (phase != RequestPhase.ENCODER) {
            return schedulerV2.cancelRequest(requestId, expectedBatchId, reason);
        }
        return requestRegistry.cancelRequest(requestId, expectedBatchId, reason, phase);
    }

    public int getTrackedRequestCount() {
        return requestRegistry.trackedRequestCount() + schedulerV2.trackedRequestCount();
    }

    public int getQueuedRequestCount() {
        return schedulerV2.queuedRequestCount() + (encoderQueue == null ? 0 : encoderQueue.size());
    }

    public int getBlockedRequestCount() {
        return schedulerV2.blockedRequestCount() + (encoderQueue == null ? 0 : encoderQueue.blockedSize());
    }

    public List<RequestState> snapshotActiveRequests() {
        List<RequestState> states = new ArrayList<>(requestRegistry.snapshotActiveRequests());
        states.addAll(schedulerV2.snapshotActiveRequests());
        states.sort(Comparator.comparingLong(RequestState::createdAtMs).thenComparing(RequestState::requestId));
        return states;
    }

    public RequestState getRequestState(String requestId, long expectedBatchId) {
        return schedulerV2.getRequestState(requestId, expectedBatchId);
    }

    /**
     * Read the Encoder or Generation lifecycle selected by phase.
     */
    public RequestState getRequestState(String requestId, long expectedBatchId, RequestPhase phase) {
        if (phase != RequestPhase.ENCODER) {
            return schedulerV2.getRequestState(requestId, expectedBatchId);
        }
        return requestRegistry.getRequestState(requestId, expectedBatchId, phase);
    }

    public void closePlacement() {
        schedulerV2.close();
        if (encoderQueue != null) {
            encoderQueue.close();
        }
    }

    private static Response error(StrategyErrorType type, String detail) {
        return Response.buildErrorResponse(type, detail);
    }
}
