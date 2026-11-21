package org.flexlb.config;

import lombok.Getter;
import lombok.Setter;

/**
 * QUEUE scheduler settings.
 */
@Getter
@Setter
public final class SchedulerConfig {

    public enum Type {
        QUEUE
    }

    private Type type = Type.QUEUE;
    /** Maximum time a request may remain owned by the QUEUE scheduler. */
    private long queueTimeoutMs = 3_600_000L;
    /**
     * Maximum Generation requests waiting for or undergoing global placement.
     */
    private int maxQueuedRequests = 100_000;
    /**
     * Maximum attempts to add a request to the NON_BATCH_V2 main queue, including the first attempt.
     */
    private int mainQueueMaxAddAttempts = 3;

    private QueueOrderingConfig ordering = new QueueOrderingConfig();

    public long resolveExpiresAtMs(long admissionTimeMs) {
        return admissionTimeMs > Long.MAX_VALUE - queueTimeoutMs
                ? Long.MAX_VALUE
                : admissionTimeMs + queueTimeoutMs;
    }
}
