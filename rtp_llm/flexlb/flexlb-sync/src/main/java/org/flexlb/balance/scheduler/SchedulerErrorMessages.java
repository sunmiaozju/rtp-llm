package org.flexlb.balance.scheduler;

/**
 * Shared error messages returned by the request schedulers.
 */
public final class SchedulerErrorMessages {

    public static final String QUEUE_FULL = "Too many requests. Your requests are being throttled"
            + " due to system capacity limits. Please try again later.";
    public static final String SCHEDULER_SHUTTING_DOWN = "request scheduler is shutting down";
    public static final String DEADLINE_EXPIRED_BEFORE_PLACEMENT =
            "request scheduling deadline has expired before placement";

    private SchedulerErrorMessages() {
    }

    public static String duplicateRequestId(String requestId) {
        return "duplicate request_id: " + requestId;
    }

    public static String preemptedByHigherPriority(String incomingRequestId) {
        return "preempted by higher-priority request " + incomingRequestId;
    }
}
