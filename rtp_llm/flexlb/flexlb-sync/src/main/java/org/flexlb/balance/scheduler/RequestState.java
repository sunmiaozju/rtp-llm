package org.flexlb.balance.scheduler;

/** Immutable public view of one canonical request generation. */
public record RequestState(
        String requestId,
        long numericRequestId,
        Phase state,
        DeliveryClaimKind deliveryClaimKind,
        long batchId,
        long createdAtMs,
        long updatedAtMs,
        String detail) {

    public RequestState {
        if (batchId != 0L) {
            throw new IllegalArgumentException("route decisions do not carry a batchId");
        }
    }

    /**
     * A legacy batch-specific query or cancel must not match a route-only request.
     */
    public boolean matchesBatch(long expectedBatchId) {
        return expectedBatchId == 0L || batchId == expectedBatchId;
    }

    public enum Phase {
        QUEUED,
        DISPATCHING,
        ACKNOWLEDGED,
        CANCEL_REQUESTED,
        CANCELLED,
        TIMED_OUT,
        FAILED,
        COMPLETED;

        boolean canTransitionTo(Phase next) {
            if (this == next) {
                return true;
            }
            return switch (this) {
                case QUEUED -> next == DISPATCHING
                        || next == CANCEL_REQUESTED
                        || next == TIMED_OUT
                        || next == FAILED;
                case DISPATCHING -> next == ACKNOWLEDGED
                        || next == CANCEL_REQUESTED
                        || next == TIMED_OUT
                        || next == FAILED
                        || next == COMPLETED;
                case ACKNOWLEDGED -> next == CANCEL_REQUESTED
                        || next == TIMED_OUT
                        || next == FAILED
                        || next == COMPLETED;
                case CANCEL_REQUESTED -> next == CANCELLED
                        || next == TIMED_OUT
                        || next == FAILED
                        || next == COMPLETED;
                case CANCELLED, TIMED_OUT, FAILED, COMPLETED -> false;
            };
        }

        public boolean isTerminal() {
            return this == CANCELLED || this == TIMED_OUT
                    || this == FAILED || this == COMPLETED;
        }
    }
}
