package org.flexlb.enums;

/**
 * Auto-TPM decode admission phase of one request tracked by the Master
 * (design doc 10.1). Ordinal order is the eviction stage order
 * (design doc 11.3): earlier phases are cheaper to evict.
 *
 * <p>A request still in the Master queue can be removed locally. Once its
 * dispatch may have reached Prefill, FlexLB must return the victim ID to the
 * caller instead of removing the request locally.</p>
 */
public enum DecodeTaskPhase {

    /** Still owned exclusively by the Master queue; Engine cannot have seen it. */
    MASTER_QUEUED_NOT_DISPATCHED,

    /**
     * Dispatch started but the request has not been confirmed by WorkerStatus.
     */
    ENGINE_MAY_HAVE_SEEN,

    /** Engine accepted the request and allocated KV, but has not started it. */
    ACCEPTED_NOT_RUNNING,

    /**
     * Engine is running the request.
     */
    RUNNING;

    /**
     * Whether dispatch may have reached Prefill.
     */
    public boolean isEngineFacing() {
        return this == ENGINE_MAY_HAVE_SEEN
                || this == ACCEPTED_NOT_RUNNING
                || this == RUNNING;
    }

    /** Whether the phase is represented by Engine WorkerStatus. */
    public boolean isEngineConfirmed() {
        return this == ACCEPTED_NOT_RUNNING || this == RUNNING;
    }

    public boolean isMasterQueued() {
        return this == MASTER_QUEUED_NOT_DISPATCHED;
    }
}
