package org.flexlb.balance.endpoint;

import lombok.Getter;
import org.flexlb.dao.master.WorkerStatus;

import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Primary abstraction for a remote inference worker.
 * Holds one immutable-generation {@link WorkerStatus} reference — all state
 * (identity, engine metrics, topology) is carried by the status object.
 *
 * <p>Callers read dynamic engine state via {@link #getStatus()} and
 * operate on it directly.
 */
public class WorkerEndpoint {

    private static final Runnable NO_STATUS_NOTIFIER = () -> { };

    private static final AtomicLong RETIREMENT_THREAD_SEQUENCE =
            new AtomicLong();
    private static final ExecutorService RETIREMENT_EXECUTOR =
            Executors.newFixedThreadPool(4, task -> {
                Thread thread = new Thread(
                        task,
                        "flexlb-endpoint-retirement-"
                                + RETIREMENT_THREAD_SEQUENCE.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });

    /**
     * Returns the underlying {@link WorkerStatus} reference.
     * Callers read only committed dynamic engine state from it.
     */
    @Getter
    private final WorkerStatus status;
    private final EndpointGenerationLifecycle generationLifecycle;

    public WorkerEndpoint(WorkerStatus status) {
        this.status = status;
        this.generationLifecycle = new EndpointGenerationLifecycle();
    }

    /**
     * 节点进入选路目录后启动其运行任务。
     */
    void onPublished() {
    }

    // ==================== identity (delegated to status) ====================

    /**
     * Returns the logical worker identity, including the engine index.
     * Network calls use the separate IP and transport port accessors.
     */
    public String ipPort() {
        return status.getLogicalIpPort();
    }

    public String getIp() {
        return status.getIp();
    }

    public int getHttpPort() {
        return status.getPort();
    }

    public int getGrpcPort() {
        return status.getGrpcPort();
    }

    // ==================== status ====================

    /**
     * Reduce one private status observation and publish it at the role's
     * consistency boundary. Any returned scheduler facts were derived from
     * exact endpoint-owned identities and are consumed only after this method
     * has released the role-local lock.
     */
    public Runnable applyPreparedStatus(
            WorkerStatus ws,
            WorkerStatus.PreparedStatus prepared) {
        requireStatusGeneration(ws);
        if (!prepared.observation().alive()) {
            beginRetirement();
        }
        ws.publishPreparedStatus(prepared);
        return NO_STATUS_NOTIFIER;
    }

    /**
     * Initialize role-local state on a candidate which is not yet routable.
     * Publication is owned by {@link EndpointRegistry} after all reducers
     * succeed.
     */
    public Runnable initializeFromPreparedStatus(
            WorkerStatus ws,
            WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        return NO_STATUS_NOTIFIER;
    }

    /**
     * Derive active-request ownership from a successful same-version status
     * heartbeat without replaying versioned endpoint state or terminal facts.
     */
    public Runnable observeStatusHeartbeat(
            WorkerStatus ws,
            WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        if (observation.owner() != ws) {
            throw new IllegalArgumentException(
                    "Status observation belongs to another WorkerStatus generation");
        }
        return NO_STATUS_NOTIFIER;
    }

    protected final void requireStatusGeneration(WorkerStatus ws) {
        if (status != ws) {
            throw new IllegalArgumentException(
                    "WorkerStatus generation does not belong to this endpoint");
        }
    }

    public boolean isRetired() {
        return generationLifecycle.isRetiringOrRetired();
    }

    /**
     * Close the admission gate for this exact endpoint generation.
     *
     * <p>The registry marks the endpoint before withdrawing its address from routing.
     * Requests that already chose the endpoint may still publish their route.</p>
     */
    final void beginRetirement() {
        generationLifecycle.beginRetirement();
    }

    /**
     * Initiate exact-generation drain and cleanup after the admission gate has
     * closed. This method never waits for cleanup already owned by another
     * caller; use {@link #awaitRetirement()} at a composition shutdown barrier.
     */
    public final void close() {
        retireInternal(false);
    }

    /** Initiate unpublished-candidate cleanup without blocking its status lock. */
    final void closeAsynchronously() {
        retireInternal(true);
    }

    private void retireInternal(boolean forceAsynchronous) {
        beginRetirement();

        if (!generationLifecycle.tryClaimCleanup()) {
            // Another exact caller already owns cleanup. Initiation is
            // idempotent and deliberately separate from the wait barrier.
            return;
        }

        if (forceAsynchronous) {
            scheduleRetirementCleanup();
        } else {
            finishRetirement();
        }
    }

    /**
     * Wait until this exact endpoint generation has completed all local cleanup
     * and retirement callbacks, then rethrow its recorded terminal failure.
     * {@link #close()} must have initiated cleanup first.
     */
    public final void awaitRetirement() {
        generationLifecycle.awaitRetirement();
    }

    private void finishRetirement() {
        generationLifecycle.beginCleanup();
        Throwable retirementFailure = null;
        try {
            closeEndpoint();
        } catch (Throwable failure) {
            retirementFailure = failure;
        } finally {
            generationLifecycle.completeRetirement(retirementFailure);
        }
        rethrowRetirementFailure(retirementFailure);
    }

    private void scheduleRetirementCleanup() {
        RETIREMENT_EXECUTOR.execute(
                () -> {
                    try {
                        finishRetirement();
                    } catch (Throwable cleanupFailure) {
                        org.flexlb.util.Logger.error(
                                "Endpoint generation cleanup failed after detach: {}#{}",
                                status.getIpPort(),
                                status.getGenerationId(),
                                cleanupFailure);
                    }
                });
    }

    /**
     * Release subclass resources after the endpoint stops accepting new routes.
     * This is invoked exactly once by the retirement owner.
     */
    protected void closeEndpoint() {
        // Stateless roles own no endpoint-local resources.
    }

    private static void rethrowRetirementFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "Endpoint generation retirement failed", failure);
        }
    }

    // ==================== monitoring (EP-authoritative) ====================

    /**
     * Role-specific observable load metric for monitoring.
     * <p>Prefill: identity-backed committed work (ms), absent while the
     * endpoint cannot publish a coherent complete work view.
     * <p>Decode: total active task count (confirmed running + inflight).
     */
    public OptionalLong getLoadMetric() {
        WorkerStatus.EngineObservation engine =
                status.committedEngineObservation();
        return OptionalLong.of(
                engine.runningTaskList() == null
                        ? 0L : engine.runningTaskList().size());
    }

}
