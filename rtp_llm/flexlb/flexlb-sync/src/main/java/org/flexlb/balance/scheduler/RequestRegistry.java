package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EncoderEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RequestPhase;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;

/**
 * Canonical request directory and global admission barrier.
 * ID/item entry points resolve an exact slot and dispatch one event; request
 * policy is implemented by RequestSlot. Removal invalidates the slot under
 * its monitor so retained capabilities cannot operate on a replacement.
 */
@Component
public class RequestRegistry {

    private final RequestCompletionPublisher completionPublisher;
    private final RequestTerminalCleanup terminalCleanup;

    private final ExpirationTimer expirationTimer;

    private final AtomicBoolean shuttingDown = new AtomicBoolean();

    private volatile EncoderQueueCoordinator encoderQueue;

    private final Object registrationLock = new Object();
    private final RequestSchedulerReporter requestReporter;

    private final ConcurrentMap<String, RequestSlot> encoderRequestSlots =
            new ConcurrentHashMap<>();

    private Map<String, RequestSlot> slots(RequestPhase phase) {
        return phase == RequestPhase.ENCODER ? encoderRequestSlots : Map.of();
    }
    @Autowired
    public RequestRegistry(ConfigService configService,
                           BatchSchedulerReporter reporter,
                           RequestSchedulerReporter requestReporter) {
        this.requestReporter = requestReporter;
        this.expirationTimer = new ExpirationTimer(this, configService);
        this.completionPublisher = new RequestCompletionPublisher(completionPublisherWorkers(configService));
        this.terminalCleanup = new RequestTerminalCleanup(expirationTimer, requestReporter);
    }

    synchronized void attachEncoderQueue(EncoderQueueCoordinator queue) {
        if (encoderQueue != null) { throw new IllegalStateException("Encoder queue already attached"); }
        encoderQueue = Objects.requireNonNull(queue, "queue");
    }

    void encoderCapacityChanged() {
        EncoderQueueCoordinator queue = encoderQueue;
        if (queue != null) {
            queue.capacityChanged();
        }
    }

    private static int completionPublisherWorkers(ConfigService configService) {
        try {
            FlexlbConfig config = configService.loadBalanceConfig();
            return config == null || config.getInternalRuntime() == null
                    ? 0 : config.getInternalRuntime()
                            .getBatchDispatchCompletionThreads();
        } catch (Throwable ignored) {
            return 0;
        }
    }

    boolean isCurrentSlot(RequestSlot slot) {
        return slot != null && encoderRequestSlots.get(slot.requestId()) == slot;
    }

    RequestSlot requestSlot(String requestId, RequestPhase phase) {
        return slots(phase).get(requestId);
    }

    /**
     * Transfer a selected Encoder request to its endpoint before publishing the route.
     */
    public boolean claimEncoderRoute(String requestId,
                                     CompletableFuture<Response> future,
                                     WorkerEndpoint.EndpointHold selected) {
        WorkerEndpoint endpoint = selected.endpoint();
        endpoint.checkHold(selected);
        if (!(endpoint instanceof EncoderEndpoint encoder)) { return false; }
        RequestSlot slot = requestSlot(requestId, RequestPhase.ENCODER);
        return slot != null && slot.ownsFuture(future)
                && slot.claimEncoderRoute(encoder);
    }

    /**
     * Publish the Encoder route after the endpoint has accepted request tracking.
     */
    public boolean publishEncoderRoute(String requestId, CompletableFuture<Response> future,
                                       Response routeResponse) {
        RequestSlot slot = requestSlot(requestId, RequestPhase.ENCODER);
        return slot != null && slot.ownsFuture(future)
                && slot.publishEncoderRoute(routeResponse);
    }

    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    public List<RequestSlot> snapshotSlots() {
        return new ArrayList<>(encoderRequestSlots.values());
    }

    public boolean removeExactTerminalRecord(
            RequestSlot exactSlot, long updatedBeforeMs) {
        synchronized (exactSlot) {
            if (!exactSlot.isRemovableTerminalRecord(updatedBeforeMs)
                    || !encoderRequestSlots.remove(
                            exactSlot.requestId(), exactSlot)) {
                return false;
            }
            exactSlot.detachGeneration();
            return true;
        }
    }

    void processEncoderStatus(EncoderEndpoint source, WorkerStatus.TaskObservation task, boolean finished) {
        RequestSlot slot = requestSlot(task.requestId(), RequestPhase.ENCODER);
        if (slot != null) { slot.processEncoderStatus(source, task, finished); }
    }

    void applyEncoderRetirement(EncoderEndpoint source, String requestId) {
        RequestSlot slot = requestSlot(requestId, RequestPhase.ENCODER);
        if (slot != null) { slot.recordEncoderRetirement(source); }
    }

    CompletableFuture<Response> register(BalanceContext context) {
        if (context == null || context.getRequest() == null || context.getRequestPhase() != RequestPhase.ENCODER) {
            return CompletableFuture.completedFuture(buildErrorResponse(
                    StrategyErrorType.INVALID_REQUEST, null));
        }
        if (shuttingDown.get()) {
            return CompletableFuture.completedFuture(buildErrorResponse(
                    StrategyErrorType.DISPATCH_FAILED,
                    SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN));
        }

        RequestSlot slot = null;
        boolean registered = false;
        try {
            // Never execute endpoint cleanup, timer operations or public callbacks here.
            // Slot reducers only remove from the concurrent index, never acquire this lock.
            synchronized (registrationLock) {
                var phaseSlots = encoderRequestSlots;
                if (phaseSlots.containsKey(context.getRequestId())) {
                    return CompletableFuture.completedFuture(buildErrorResponse(
                            StrategyErrorType.INVALID_REQUEST,
                            SchedulerErrorMessages.duplicateRequestId(context.getRequestId())));
                }
                if (context.requestExpired(System.currentTimeMillis())) {
                    Response failure = buildErrorResponse(StrategyErrorType.RESOURCE_EXHAUSTED,
                            SchedulerErrorMessages.DEADLINE_EXPIRED_BEFORE_PLACEMENT);
                    return CompletableFuture.completedFuture(failure);
                }
                if (shuttingDown.get()) {
                    return CompletableFuture.completedFuture(buildErrorResponse(
                            StrategyErrorType.DISPATCH_FAILED,
                            SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN));
                }
                slot = new RequestSlot(completionPublisher, context, expirationTimer, terminalCleanup);
                context.setEnqueueTime(System.currentTimeMillis());
                synchronized (slot) {
                    var requestLifecycle = context.getConfig().getRequestLifecycle().getRequest();
                    slot.configureInactivityTimeout(requestLifecycle.getTimeoutMs());
                    phaseSlots.put(context.getRequestId(), slot);
                    registered = true;
                }
            }
            RequestFuture future = slot.future();
            if (shuttingDown.get()) {
                completeError(
                        future,
                        StrategyErrorType.DISPATCH_FAILED,
                        SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN);
                return future;
            }
            if (context.requestExpired(System.currentTimeMillis())) {
                slot.cancelRequest(0L, CancelReason.DEADLINE_EXCEEDED);
                return future;
            }
            attachRequestExpiration(context, future);
            return future;
        } catch (Throwable failure) {
            requestReporter.reportLifecycleFailure("registration");
            Logger.error(
                    "Request registration failed for request id: {}",
                    context.getRequestId(),
                    failure);
            String detail = "Submit failed: " + failure.getMessage();
            if (registered) {
                completeError(
                        slot.future(),
                        StrategyErrorType.DISPATCH_FAILED,
                        detail);
                return slot.future();
            }
            return CompletableFuture.completedFuture(buildErrorResponse(
                    StrategyErrorType.DISPATCH_FAILED, detail));
        }
    }

    void attachRequestExpiration(BalanceContext context, CompletableFuture<Response> future) {
        if (shuttingDown.get()) {
            return;
        }
        RequestSlot slot = slots(context.getRequestPhase()).get(context.getRequestId());
        if (slot == null || !slot.ownsFuture(future)) {
            return;
        }
        expirationTimer.attachRequestDeadline(slot, context.getRequestExpiresAtMs());
        expirationTimer.attachInactivityDeadline(slot);
    }

    public RequestState cancelRequest(String requestId, long expectedBatchId, CancelReason reason,
                                      RequestPhase phase) {
        RequestSlot slot = requestSlot(requestId, phase);
        return slot == null ? null : slot.cancelRequest(expectedBatchId, reason);
    }

    public int trackedRequestCount() {
        return trackedRequestCount(RequestPhase.ENCODER);
    }

    public int trackedRequestCount(RequestPhase phase) {
        int live = 0;
        for (RequestSlot slot : slots(phase).values()) {
            synchronized (slot) {
                if (isCurrentSlot(slot)
                        && slot.isLiveGeneration()) {
                    live++;
                }
            }
        }
        return live;
    }

    /**
     * Collect both request counts and the oldest live age in one traversal.
     */
    public TrackedRequestStats trackedRequestStats() {
        int generation = 0;
        int encoder = 0;
        long oldest = Long.MAX_VALUE;
        long now = System.currentTimeMillis();
        for (RequestPhase phase : RequestPhase.values()) {
            if (phase != RequestPhase.GENERATION && phase != RequestPhase.ENCODER) {
                continue;
            }
            for (RequestSlot slot : slots(phase).values()) {
                synchronized (slot) {
                    if (!isCurrentSlot(slot) || !slot.isLiveGeneration()) {
                        continue;
                    }
                    if (phase == RequestPhase.GENERATION) {
                        generation++;
                    } else {
                        encoder++;
                    }
                    oldest = Math.min(oldest, slot.createdAtMs());
                }
            }
        }
        return new TrackedRequestStats(generation, encoder,
                oldest == Long.MAX_VALUE ? 0L : Math.max(0L, now - oldest));
    }

    public record TrackedRequestStats(int generationRequests, int encoderRequests, long oldestAgeMs) { }

    public List<RequestState> snapshotActiveRequests() {
        List<RequestState> snapshots = new ArrayList<>(encoderRequestSlots.size());
        for (RequestSlot entry : snapshotSlots()) {
            synchronized (entry) {
                if (isCurrentSlot(entry)
                        && entry.isLiveGeneration()) {
                    snapshots.add(entry.snapshot());
                }
            }
        }
        snapshots.sort((left, right) -> {
            int createdOrder = Long.compare(left.createdAtMs(), right.createdAtMs());
            return createdOrder != 0
                    ? createdOrder : left.requestId().compareTo(right.requestId());
        });
        return snapshots;
    }

    public RequestState getRequestState(String requestId,
                                        long expectedBatchId) {
        return getRequestState(requestId, expectedBatchId, RequestPhase.GENERATION);
    }

    public RequestState getRequestState(String requestId,
                                        long expectedBatchId, RequestPhase phase) {
        RequestSlot entry = slots(phase).get(requestId);
        if (entry == null) {
            return null;
        }
        synchronized (entry) {
            RequestState snapshot = entry.snapshot();
            return snapshot != null && snapshot.matchesBatch(expectedBatchId) ? snapshot : null;
        }
    }

    private static void completeError(CompletableFuture<Response> future,
                                      StrategyErrorType errorType,
                                      String message) {
        if (future.isDone()) {
            return;
        }
        future.complete(buildErrorResponse(errorType, message));
    }

    boolean publishDecisionResponseAsync(String requestId, CompletableFuture<Response> future,
                                         Response response, RequestPhase phase) {
        RequestSlot slot = slots(phase).get(requestId);
        return slot != null && slot.ownsFuture(future) && slot.terminateLocallyAndPublishResponse(response);
    }

    public boolean closeAdmissionAndAwaitMutations() {
        return shuttingDown.compareAndSet(false, true);
    }

    public void closeOutstandingAndTerminalize() {
        if (!shuttingDown.get()) {
            throw new IllegalStateException(
                    "admission must close before terminal shutdown");
        }
        completeOutstandingRequestsForShutdown();
    }

    public void maintainExpiration(
            BiConsumer<Long, Predicate<String>> exactSweeper) {
        expirationTimer.maintain(exactSweeper);
    }

    public void closeExpiration() {
        expirationTimer.close();
    }

    public void closePublisher() {
        completionPublisher.close();
    }

    private void completeOutstandingRequestsForShutdown() {
        List<TerminalAction> actions = new ArrayList<>();
        for (RequestSlot slot : snapshotSlots()) {
            TerminalAction action = slot.claimShutdownAction();
            if (action != null) { actions.add(action); }
        }
        actions.forEach(terminalCleanup::submitTerminal);
    }

}
