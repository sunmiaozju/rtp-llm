package org.flexlb.service;

import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.scheduler.RequestScheduler;
import org.flexlb.balance.scheduler.RequestState;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RequestPhase;
import org.flexlb.service.monitor.AsyncRequestMetrics;
import org.flexlb.telemetry.FlexlbTrace;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class RouteService {

    private final RequestScheduler requestScheduler;
    @Autowired
    private DefaultRouter router;
    private final TheoryCacheHitReporter theoryCacheHitReporter;
    private final AsyncRequestMetrics asyncRequestMetrics;

    public RouteService(RequestScheduler requestScheduler,
                        TheoryCacheHitReporter theoryCacheHitReporter,
                        AsyncRequestMetrics asyncRequestMetrics) {
        this.requestScheduler = requestScheduler;
        this.theoryCacheHitReporter = theoryCacheHitReporter;
        this.asyncRequestMetrics = asyncRequestMetrics;
    }

    /**
     * Route request to appropriate workers based on the deployment-level schedule mode.
     * @param balanceContext Load balancing context
     * @return Routing result
     */
    public CompletableFuture<Response> route(BalanceContext balanceContext) {
        if (balanceContext.getRequest() != null && balanceContext.getRequest().isVitRouteOnly()) {
            return CompletableFuture.completedFuture(router == null
                    ? Response.error(StrategyErrorType.NO_VIT_WORKER) : router.routeVit(balanceContext));
        }
        // Reject stale metadata before the scheduler creates request lifecycle ownership.
        if (balanceContext.getRequest() != null && balanceContext.getRequest().getSelectedVit() != null
                && getRequestState(balanceContext.getRequestId(), 0) == null
                && (router == null || !router.selectedVitIsValid(balanceContext))) {
            return CompletableFuture.completedFuture(Response.error(StrategyErrorType.VIT_ROUTE_STALE));
        }
        FlexlbTrace.setScheduleAttribute(balanceContext.getTraceContext(), FlexlbTrace.SCHEDULE_MODE, "QUEUE");

        CompletableFuture<Response> resultFuture = submitScheduled(balanceContext);

        // Observe the scheduler-owned future without replacing it with a
        // dependent stage. Returning the exact source preserves external
        // cancel propagation and keeps one publication owner end to end.
        resultFuture.whenComplete((result, throwable) -> {
            Long lastCacheMatchUs = balanceContext.getMetrics().getLastCacheMatchUs();
            if (lastCacheMatchUs != null) {
                asyncRequestMetrics.reportCacheMatch(lastCacheMatchUs);
            }
            if (throwable != null) {
                return;
            }
            try {
                balanceContext.setResponse(result);
                if (result != null && result.isSuccess()) {
                    theoryCacheHitReporter.report(balanceContext);
                }
            } catch (RuntimeException completionSideEffectFailure) {
                Logger.warn("Route completion side effect failed: request_id={}",
                        balanceContext.getRequestId(), completionSideEffectFailure);
            }
        });
        return resultFuture;
    }

    /**
     * Submit to the common scheduler. Route-decision requests intentionally do
     * not require generate_input: Master selects endpoints but the frontend
     * remains responsible for sending the original request to the engine.
     */
    private CompletableFuture<Response> submitScheduled(BalanceContext balanceContext) {
        if (requestScheduler == null) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "RequestScheduler is required for the configured scheduling path"));
        }
        CompletableFuture<Response> resultFuture = requestScheduler.submit(balanceContext);
        balanceContext.setFuture(resultFuture);
        return resultFuture;
    }

    public RequestState getRequestState(String requestId,
                                                    long expectedBatchId) {
        return requestScheduler == null ? null
                : requestScheduler.getRequestState(requestId, expectedBatchId);
    }

    /**
     * Read the selected phase; callers omitting phase use Generation.
     */
    public RequestState getRequestState(
            String requestId, long expectedBatchId, RequestPhase phase) {
        return requestScheduler == null ? null
                : requestScheduler.getRequestState(requestId, expectedBatchId, phase);
    }

    /**
     * Cancel one scheduler-owned request generation.
     *
     * <p>The scheduler is the only lifecycle and resource owner.  Keeping the
     * reducer there gives BATCH enqueue and QUEUE route-decision delivery the
     * same idempotency and generation-fencing semantics.</p>
     */
    public RequestState cancelRequest(String requestId,
                                                   long expectedBatchId,
                                                   CancelReason reason) {
        return requestScheduler == null ? null
                : requestScheduler.cancelRequest(requestId, expectedBatchId, reason);
    }

    /**
     * Cancel the selected phase locally. Encoder cancellation sends no engine RPC.
     */
    public RequestState cancelRequest(
            String requestId, long expectedBatchId, CancelReason reason, RequestPhase phase) {
        return requestScheduler == null ? null
                : requestScheduler.cancelRequest(requestId, expectedBatchId, reason, phase);
    }

}
