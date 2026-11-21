package org.flexlb.balance.endpoint;

/**
 * One endpoint cleanup and its completion state.
 */
public final class EndpointGenerationLifecycle {

    private static final long DEFAULT_RETIREMENT_TIMEOUT_MS =
            Long.getLong("flexlb.endpoint.retirement.timeout.ms", 30_000L);

    private enum RetirementPhase {
        ACTIVE,
        RETIRING,
        RETIRED
    }

    private volatile RetirementPhase phase = RetirementPhase.ACTIVE;
    private boolean cleanupClaimed;
    private Thread cleanupThread;
    private Throwable retirementFailure;

    boolean isRetiringOrRetired() {
        return phase != RetirementPhase.ACTIVE;
    }

    /** Close the gate without waiting or running endpoint cleanup. */
    synchronized void beginRetirement() {
        if (phase == RetirementPhase.ACTIVE) {
            phase = RetirementPhase.RETIRING;
        }
    }

    /** Exactly one caller may own the endpoint-local drain and cleanup. */
    synchronized boolean tryClaimCleanup() {
        if (phase == RetirementPhase.ACTIVE) {
            throw new IllegalStateException(
                    "endpoint retirement gate is still open");
        }
        if (phase == RetirementPhase.RETIRED || cleanupClaimed) {
            return false;
        }
        cleanupClaimed = true;
        return true;
    }

    /** Bind the claimed cleanup to its execution thread for reentrant close. */
    synchronized void beginCleanup() {
        if (phase != RetirementPhase.RETIRING || !cleanupClaimed || cleanupThread != null) {
            throw new IllegalStateException(
                    "endpoint retirement cleanup owner is invalid");
        }
        cleanupThread = Thread.currentThread();
    }

    synchronized void completeRetirement(Throwable failure) {
        if (phase != RetirementPhase.RETIRING) {
            throw new IllegalStateException("endpoint generation is not retiring");
        }
        retirementFailure = failure;
        cleanupThread = null;
        phase = RetirementPhase.RETIRED;
        notifyAll();
    }

    void awaitRetirement() {
        awaitRetirement(DEFAULT_RETIREMENT_TIMEOUT_MS);
    }

    void awaitRetirement(long timeoutMs) {
        if (timeoutMs <= 0L) {
            throw new IllegalArgumentException(
                    "endpoint retirement timeout must be positive");
        }
        boolean interrupted = false;
        Throwable failure = null;
        IllegalStateException timeoutFailure = null;
        long deadlineNanos = System.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (this) {
            if (phase == RetirementPhase.ACTIVE) {
                throw new IllegalStateException(
                        "endpoint retirement has not begun");
            }
            if (phase == RetirementPhase.RETIRING && !cleanupClaimed) {
                throw new IllegalStateException(
                        "endpoint retirement cleanup has not been initiated");
            }
            if (phase == RetirementPhase.RETIRING
                    && cleanupThread == Thread.currentThread()) {
                throw new IllegalStateException(
                        "endpoint retirement cleanup cannot await itself");
            }
            while (phase != RetirementPhase.RETIRED) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L) {
                    timeoutFailure = new IllegalStateException("endpoint retirement timed out after " + timeoutMs
                            + "ms: phase=" + phase + ", cleanupClaimed=" + cleanupClaimed);
                    break;
                }
                try {
                    long waitMillis = Math.max(
                            1L,
                            java.util.concurrent.TimeUnit.NANOSECONDS
                                    .toMillis(remainingNanos));
                    wait(waitMillis);
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
            if (timeoutFailure == null) {
                failure = retirementFailure;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (timeoutFailure != null) {
            throw timeoutFailure;
        }
        rethrow(failure);
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "endpoint generation retirement failed", failure);
        }
    }

}
