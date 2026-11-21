package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EncoderEndpoint;
import org.flexlb.balance.scheduler.ExpirationTimer.InactivityDeadline;
import org.flexlb.balance.scheduler.ExpirationTimer.RequestDeadline;
import org.flexlb.balance.scheduler.RequestCompletionPublisher.PublicationPermit;
import org.flexlb.balance.scheduler.RequestCompletionPublisher.ResponseCompletion;
import org.flexlb.balance.scheduler.RequestCompletionPublisher.SelectedPublication;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.RejectReason;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.WorkerStatus;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static org.flexlb.balance.scheduler.RequestTerminalCleanup.appendFailure;
import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;

/**
 * Tracks one Encoder request through placement, route publication and worker completion.
 */
public final class RequestSlot {

    private final ExpirationTimer expirationTimer;
    private final RequestTerminalCleanup terminalCleanup;

    private final RequestCompletionPublisher completionPublisher;
    /** Used while active to capture PV evidence; released with the terminal record. */
    private BalanceContext context;
    private final String requestId;
    private final long numericRequestId;
    private EncoderEndpoint encoderEndpoint;
    private final long createdAtMs;
    private final RequestFuture future;
    private RequestState.Phase state = RequestState.Phase.QUEUED;
    private long updatedAtMs;
    private String detail = "queued";
    private DeliveryClaimKind deliveryClaimKind = DeliveryClaimKind.NONE;
    /** Cleared exactly when the directory removes this request generation. */
    private boolean currentGeneration = true;

    private SlotPhase slotPhase = SlotPhase.ACTIVE;
    /** Frozen with the first cancellation; later cleanup cannot reclassify it. */
    private Response cancellationResponse;
    private CancelReason cancellationReason;
    /** One frontend result may win before its unlocked future completion runs. */
    private PublicationKind publicationWinner;

    private boolean admissionOpen = true;
    private RequestDeadline requestDeadline;
    private InactivityDeadline inactivityDeadline;
    private long inactivityTimeoutMs;
    private long lastWorkerStatusAtMs;

    RequestSlot(RequestCompletionPublisher completionPublisher,
                BalanceContext context,
                ExpirationTimer expirationTimer,
                RequestTerminalCleanup terminalCleanup) {
        this.expirationTimer = expirationTimer;
        this.terminalCleanup = terminalCleanup;
        this.completionPublisher = completionPublisher;
        this.context = context;
        this.requestId = context.getRequestId();
        this.numericRequestId = context.getNumericRequestId();
        this.createdAtMs = System.currentTimeMillis();
        this.updatedAtMs = createdAtMs;
        this.lastWorkerStatusAtMs = createdAtMs;
        this.future = new RequestFuture(this);
    }

    // ── 请求身份与状态：只读查询、生命周期条件和状态提交 ──

    String requestId() {
        return requestId;
    }

    boolean claimEncoderRoute(EncoderEndpoint endpoint) {
        synchronized (this) {
            if (!isOpen()
                    || state != RequestState.Phase.QUEUED || encoderEndpoint != null) {
                return false;
            }
            long seqLen = Math.max(0L, context.getRequest().getSeqLen());
            Long cacheHit = context.getRequest().getEncoderCacheHitLen();
            long estimatedUncached = seqLen - Math.min(seqLen,
                    Math.max(0L, cacheHit == null ? 0L : cacheHit));
            if (!endpoint.trackSelectedRequest(requestId, estimatedUncached)) { return false; }
            encoderEndpoint = endpoint;
            deliveryClaimKind = DeliveryClaimKind.ROUTE_DECISION;
            transitionLocked(RequestState.Phase.DISPATCHING, "route decision delivery started");
            return true;
        }
    }

    boolean publishEncoderRoute(Response response) {
        if (!response.isSuccess()) {
            throw new IllegalArgumentException("Encoder route requires a successful response");
        }
        SelectedPublication publication;
        synchronized (this) {
            if (!isOpen()
                    || state != RequestState.Phase.DISPATCHING || encoderEndpoint == null
                    || cancellationReason != null) {
                return false;
            }
            PublicationPermit permit = requirePublicationPermitLocked(PublicationKind.DELIVERY);
            try {
                transitionLocked(RequestState.Phase.ACKNOWLEDGED, "route decision delivered");
                publicationWinner = PublicationKind.DELIVERY;
                permit.claim();
                publication = new SelectedPublication(permit, future, true,
                        ResponseCompletion.RESPONSE, response, null, false);
            } catch (RuntimeException | Error failure) {
                permit.abandonIfUnclaimed();
                throw failure;
            }
        }
        completionPublisher.submit(publication);
        return true;
    }

    void processEncoderStatus(EncoderEndpoint source, WorkerStatus.TaskObservation task, boolean finished) {
        TerminalAction action = null;
        synchronized (this) {
            if (!ownsResourceTrackingLocked()
                    || encoderEndpoint != source || !requestId.equals(task.requestId())) {
                return;
            }
            lastWorkerStatusAtMs = System.currentTimeMillis();
            if (finished) {
                TerminalOutcome result = task.errorCode() == 0L
                        ? TerminalOutcome.complete("encoder completed")
                        : TerminalOutcome.fail("encoder worker error code " + task.errorCode());
                action = claimTerminalActionLocked(result, null, false);
            }
        }
        if (!finished) { expirationTimer.attachInactivityDeadline(this); }
        if (action != null) {
            terminalCleanup.submitTerminal(action);
        }
    }

    RequestFuture future() {
        return future;
    }

    long createdAtMs() {
        return createdAtMs;
    }

    boolean ownsFuture(CompletableFuture<?> expected) {
        return future == expected;
    }

    synchronized RequestState snapshot() {
        return new RequestState(
                requestId, numericRequestId, state, deliveryClaimKind, 0L,
                createdAtMs, updatedAtMs, detail);
    }

    synchronized boolean isOpen() {
        return slotPhase == SlotPhase.ACTIVE
                && admissionOpen
                && !future.isDone()
                && !state.isTerminal();
    }

    synchronized boolean isLiveGeneration() {
        return currentGeneration && slotPhase != SlotPhase.TERMINAL_RECORD;
    }

    synchronized boolean isRemovableTerminalRecord(long updatedBeforeMs) {
        return currentGeneration
                && slotPhase == SlotPhase.TERMINAL_RECORD
                && state.isTerminal()
                && updatedAtMs < updatedBeforeMs;
    }

    synchronized void detachGeneration() {
        if (!currentGeneration) {
            throw new IllegalStateException(
                    "request generation already detached: " + requestId);
        }
        currentGeneration = false;
    }

    private boolean ownsActiveGenerationLocked() {
        requireSlotLock("active generation lookup");
        return currentGeneration
                && slotPhase == SlotPhase.ACTIVE
                && !state.isTerminal();
    }

    private boolean ownsResourceTrackingLocked() {
        requireSlotLock("resource tracking lookup");
        return currentGeneration && slotPhase == SlotPhase.ACTIVE;
    }

    private void ensureTransitionAllowedLocked(RequestState.Phase next) {
        if (!state.canTransitionTo(next)) {
            throw new IllegalStateException(
                    "invalid request lifecycle transition "
                            + state + " -> " + next);
        }
    }

    private RequestState transitionLocked(
            RequestState.Phase next,
            String message) {
        if (state == next) {
            return snapshot();
        }
        ensureTransitionAllowedLocked(next);
        state = next;
        detail = message == null ? "" : message;
        updatedAtMs = System.currentTimeMillis();
        return snapshot();
    }

    private RequestState commitTerminalStateLocked(TerminalOutcome outcome) {
        requireSlotLock("terminal state commitment");
        if (state.isTerminal()) { return snapshot(); }
        if (outcome.phase() == RequestState.Phase.CANCELLED && state != RequestState.Phase.CANCEL_REQUESTED) {
            transitionLocked(RequestState.Phase.CANCEL_REQUESTED, outcome.detail());
        }
        return transitionLocked(outcome.phase(), outcome.detail());
    }

    public RequestState cancelRequest(long expectedBatchId, CancelReason reason) {
        TerminalAction action = null;
        RequestState result;
        synchronized (this) {
            if (!currentGeneration || !snapshot().matchesBatch(expectedBatchId)) { return null; }
            if (reason == CancelReason.DEADLINE_EXCEEDED && deliveryClaimKind != DeliveryClaimKind.NONE) {
                return snapshot();
            }
            if (recordCancellationLocked(reason, reason.getMessage())) {
                action = tryTerminateCancellationLocked();
            }
            result = snapshot();
        }
        terminalCleanup.submitTerminal(action);
        return result;
    }

    /** Record the first cancellation only. Resource ownership is unchanged. */
    private boolean recordCancellationLocked(CancelReason reason, String message) {
        requireSlotLock("record cancellation");
        if (!ownsActiveGenerationLocked() || cancellationReason != null) {
            return false;
        }
        if (reason == CancelReason.DEADLINE_EXCEEDED) {
            String cause = deliveryClaimKind == DeliveryClaimKind.NONE ? "waiting for Encoder placement" : message;
            cancellationResponse = Response.error(StrategyErrorType.RESOURCE_EXHAUSTED, RejectReason.RESOURCE_EXHAUSTED,
                    cause);
            context.setSchedulingDiagnostics(Map.of("cause", cause));
        } else {
            cancellationResponse = buildErrorResponse(StrategyErrorType.REQUEST_CANCELLED, message);
        }
        cancellationReason = reason;
        admissionOpen = false;
        transitionLocked(RequestState.Phase.CANCEL_REQUESTED, message);
        return true;
    }

    private TerminalAction tryTerminateCancellationLocked() {
        return claimTerminalActionLocked(TerminalOutcome.cancellation(cancellationReason, cancellationReason.getMessage()),
                cancellationResponse, true);
    }

    TerminalAction claimShutdownAction() {
        synchronized (this) {
            if (!currentGeneration || !canClaimLocalTerminalLocked()) { return null; }
            String message = SchedulerErrorMessages.SCHEDULER_SHUTTING_DOWN;
            return claimTerminalActionLocked(TerminalOutcome.fail(message),
                    buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, message), true);
        }
    }

    private boolean canClaimLocalTerminalLocked() {
        requireSlotLock("local terminal eligibility");
        return ownsActiveGenerationLocked()
                && !future.isDone()
                && state != RequestState.Phase.ACKNOWLEDGED
                && !deliveryClaimKind.isClaimed();
    }

    // ── 调度期限：安装与到期 ──

    synchronized boolean installRequestDeadline(RequestDeadline exact) {
        if (!ownsActiveGenerationLocked() || !isOpen()) {
            return false;
        }
        if (requestDeadline != null) {
            throw new IllegalStateException(
                    "request deadline already installed for " + requestId);
        }
        requestDeadline = exact;
        return true;
    }

    void onSchedulingDeadline(RequestDeadline exact) {
        TerminalAction action = null;
        synchronized (this) {
            if (requestDeadline != exact) {
                return;
            }
            requestDeadline = null;
            if (!ownsActiveGenerationLocked() || !isOpen()) {
                return;
            }
            admissionOpen = false;
            if (deliveryClaimKind == DeliveryClaimKind.NONE && recordCancellationLocked(CancelReason.DEADLINE_EXCEEDED,
                    CancelReason.DEADLINE_EXCEEDED.getMessage())) {
                action = tryTerminateCancellationLocked();
            }
        }
        terminalCleanup.submitTerminal(action);
    }

    // ── 沉默期限：计划、安装、消费与显式检查 ──

    void configureInactivityTimeout(long timeoutMs) {
        if (timeoutMs <= 0L) {
            throw new IllegalArgumentException("request inactivity timeout must be positive");
        }
        inactivityTimeoutMs = timeoutMs;
    }

    OptionalLong inactivityDeadlineAtMs() {
        return ownsResourceTrackingLocked() && inactivityDeadline == null
                && inactivityTimeoutMs > 0L
                ? OptionalLong.of(inactivityExpiresAtMsLocked())
                : OptionalLong.empty();
    }

    boolean installInactivityDeadline(InactivityDeadline exact) {
        if (inactivityDeadlineAtMs().isEmpty()) {
            return false;
        }
        inactivityDeadline = exact;
        return true;
    }

    void onInactivityDeadline(InactivityDeadline exact, long nowMs) {
        TerminalAction action = null;
        synchronized (this) {
            if (inactivityDeadline != exact || !ownsResourceTrackingLocked()) {
                return;
            }
            inactivityDeadline = null;
            if (nowMs >= inactivityExpiresAtMsLocked()) {
                String message = "REQUEST_INACTIVE: no matching Engine request status before inactivity timeout";
                recordCancellationLocked(CancelReason.DEADLINE_EXCEEDED, message);
                action = claimTerminalActionLocked(TerminalOutcome.cancellation(cancellationReason, message),
                        cancellationResponse, true);
            }
        }
        terminalCleanup.submitTerminal(action);
    }

    private long inactivityExpiresAtMsLocked() {
        return lastWorkerStatusAtMs > Long.MAX_VALUE - inactivityTimeoutMs
                ? Long.MAX_VALUE : lastWorkerStatusAtMs + inactivityTimeoutMs;
    }

    /** The only close gate: no event may discard an outstanding cleanup obligation. */
    private TerminalAction claimTerminalActionLocked(TerminalOutcome transition, Response response,
            boolean requestPublication) {
        requireSlotLock("terminal claim");
        if (transition == null) {
            throw new IllegalStateException(
                    "terminal transition is required for request " + requestId);
        }
        if (!ownsResourceTrackingLocked()) {
            return null;
        }
        boolean publishable = requestPublication
                && publicationWinner == null
                && !future.isDone();
        PublicationPermit publication = publishable
                ? requirePublicationPermitLocked(PublicationKind.TERMINAL) : null;
        boolean transferred = false;
        try {
            slotPhase = SlotPhase.TERMINALIZING;
            admissionOpen = false;
            if (publication != null) {
                publicationWinner = PublicationKind.TERMINAL;
            }

            RequestDeadline claimedRequestDeadline = requestDeadline;
            requestDeadline = null;
            InactivityDeadline claimedInactivityDeadline = inactivityDeadline;
            inactivityDeadline = null;
            ExpirationTimer.DetachedRequestTimers terminalResources = new ExpirationTimer.DetachedRequestTimers(
                    claimedRequestDeadline, claimedInactivityDeadline);
            TerminalAction action = new TerminalAction(this, terminalResources, transition,
                    publishable ? response : null, publication);
            transferred = true;
            return action;
        } finally {
            if (!transferred && publication != null) {
                publication.abandonIfUnclaimed();
            }
        }
    }

    synchronized void requireCleanupOwner(TerminalAction action) {
        if (slotPhase != SlotPhase.TERMINALIZING || action.slot() != this) {
            throw new IllegalStateException("cleanup does not own request " + requestId);
        }
    }

    synchronized TerminationResult commitTerminalRecord(TerminalAction action) {
        if (!currentGeneration
                || slotPhase != SlotPhase.TERMINALIZING
                || action.slot() != this) {
            if (action.publication() != null) {
                action.publication().abandonIfUnclaimed();
            }
            return new TerminationResult(
                    null,
                    new IllegalStateException(
                            "terminal slot identity changed: request_id="
                                    + requestId),
                    null);
        }
        RequestState terminal;
        Throwable transitionFailure = null;
        try {
            terminal = commitTerminalStateLocked(action.transition());
        } catch (Throwable failure) {
            transitionFailure = failure;
            terminal = commitTerminalStateLocked(TerminalOutcome.fail("terminal transition failed"));
        }
        if (!terminal.state().isTerminal()) {
            transitionFailure = appendFailure(
                    transitionFailure,
                    new IllegalStateException(
                            "terminal transition did not terminate request "
                                    + requestId));
            terminal = commitTerminalStateLocked(TerminalOutcome.fail("terminal transition did not terminate"));
        }

        if (encoderEndpoint != null) {
            encoderEndpoint.forgetRequest(requestId);
            encoderEndpoint = null;
        }
        context = null;
        cancellationReason = null;
        requestDeadline = null;
        inactivityDeadline = null;
        slotPhase = SlotPhase.TERMINAL_RECORD;
        return new TerminationResult(
                terminal,
                transitionFailure,
                action.publication());
    }

    // ── 响应：本地结束、结果仲裁与发布交接 ──

    boolean terminateLocallyAndPublishResponse(Response response) {
        PublicationPermit permit = terminateLocallyAndAcquirePublication(responseOutcome(response));
        if (permit == null) { return false; }
        completionPublisher.submit(selectPublication(permit, ResponseCompletion.RESPONSE, response, null, false));
        return true;
    }

    boolean completeExternal(ResponseCompletion completion, Response response, Throwable error, boolean interrupt) {
        requireOutsideSlotLock("external Future completion");
        TerminalOutcome outcome = switch (completion) {
            case RESPONSE -> responseOutcome(response);
            case FAILURE -> {
                yield TerminalOutcome.fail("external future failure"
                        + (error.getMessage() == null ? "" : ": " + error.getMessage()));
            }
            case CANCELLATION -> TerminalOutcome.cancel(CancelReason.CLIENT_CANCELLED.getMessage());
        };
        PublicationPermit permit = terminateLocallyAndAcquirePublication(outcome);
        return permit != null && completionPublisher.publishNow(
                selectPublication(permit, completion, response, error, interrupt));
    }

    private static TerminalOutcome responseOutcome(Response response) {
        String detail = response != null && response.getErrorMessage() != null
                ? response.getErrorMessage() : "external future completion";
        return response != null && !response.isSuccess() ? TerminalOutcome.fail(detail) : TerminalOutcome.complete(detail);
    }

    private PublicationPermit terminateLocallyAndAcquirePublication(TerminalOutcome transition) {
        TerminalAction action;
        synchronized (this) {
            if (!currentGeneration || !canClaimLocalTerminalLocked()) { return null; }
            action = claimTerminalActionLocked(transition, null, true);
        }
        return action == null ? null : terminalCleanup.finishTerminal(action);
    }

    private PublicationPermit requirePublicationPermitLocked(
            PublicationKind kind) {
        PublicationPermit permit = completionPublisher.tryReservePublication(
                this, kind);
        if (permit == null || permit.slot() != this) {
            if (permit != null) {
                permit.abandonIfUnclaimed();
            }
            throw new IllegalStateException(
                    "frontend publication is closed for request " + requestId);
        }
        return permit;
    }

    SelectedPublication selectPublication(PublicationPermit permit, ResponseCompletion completion,
            Response response, Throwable failure, boolean mayInterruptIfRunning) {
        requireOutsideSlotLock("response selection");
        if (permit.slot != this || completion != ResponseCompletion.RESPONSE && permit.kind != PublicationKind.TERMINAL) {
            throw new IllegalArgumentException("incompatible publication permit");
        }
        permit.claim();
        try {
            synchronized (this) {
                return new SelectedPublication(permit, future, claimPublicationResultLocked(permit.kind), completion,
                        response, failure, mayInterruptIfRunning);
            }
        } catch (RuntimeException | Error selectionFailure) {
            permit.closePublication();
            throw selectionFailure;
        }
    }

    /** Select the response under the slot lock; complete its future only after unlocking. */
    private boolean claimPublicationResultLocked(PublicationKind kind) {
        requireSlotLock("frontend result selection");
        if (future.isDone()) {
            return false;
        }
        if (kind == PublicationKind.TERMINAL) {
            return publicationWinner == PublicationKind.TERMINAL;
        }
        if (publicationWinner != null || !ownsActiveGenerationLocked()
                || state != RequestState.Phase.ACKNOWLEDGED
                || cancellationReason != null) {
            return false;
        }
        publicationWinner = PublicationKind.DELIVERY;
        return true;
    }

    void submitTerminalResponse(PublicationPermit permit, Response response) {
        completionPublisher.submit(selectPublication(permit, ResponseCompletion.RESPONSE, response, null, false));
    }

    // ── 锁边界断言 ──

    private void requireSlotLock(String operation) {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(
                    operation + " requires slot lock for request " + requestId);
        }
    }

    private void requireOutsideSlotLock(String operation) {
        if (Thread.holdsLock(this)) {
            throw new IllegalStateException(operation + " must run outside the RequestSlot lock");
        }
    }

    private enum SlotPhase {
        ACTIVE, TERMINALIZING, TERMINAL_RECORD
    }

    enum PublicationKind {
        DELIVERY, TERMINAL
    }

    synchronized ExpirationTimer.DetachedDeadlines detachDeadlinesForTimerClose() {
        var deadlines = new ExpirationTimer.DetachedDeadlines(requestDeadline, inactivityDeadline);
        requestDeadline = null;
        inactivityDeadline = null;
        return deadlines;
    }
}

record TerminalAction(
        RequestSlot slot,
        ExpirationTimer.DetachedRequestTimers terminalResources,
        TerminalOutcome transition,
        Response response,
        PublicationPermit publication) {
}

record TerminationResult(
        RequestState terminal,
        Throwable transitionFailure,
        RequestCompletionPublisher.PublicationPermit publication) {
}
