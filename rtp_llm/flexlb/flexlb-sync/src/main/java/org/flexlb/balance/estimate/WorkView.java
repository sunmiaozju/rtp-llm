package org.flexlb.balance.estimate;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Immutable view of Prefill work which has crossed an endpoint lifecycle boundary.
 *
 * <p>Requests observed only through aggregate engine counters have no known duration.
 */
public final class WorkView {

    private static final long[] EMPTY_LONGS = new long[0];
    private static final String[] EMPTY_STRINGS = new String[0];

    private final long createdAtMs;
    private final List<RequestWork> requests;
    private final long unknownRequestCount;
    private final long knownNonRunningWorkMs;
    private final long[] runningWorkMs;
    private final String[] requestIds;

    public WorkView(long createdAtMs, List<RequestWork> requests, long unknownRequestCount) {
        this.createdAtMs = createdAtMs;
        this.requests = requests;
        if (unknownRequestCount < 0L) {
            throw new IllegalArgumentException(
                    "unknownRequestCount must be non-negative");
        }
        this.unknownRequestCount = unknownRequestCount;

        int runningCount = 0;
        int requestIdCount = this.requests.size();
        long nonRunningMs = 0L;
        for (RequestWork request : this.requests) {
            if (request.phase() == Phase.ENGINE_RUNNING) {
                runningCount++;
            } else {
                nonRunningMs = saturatedAdd(
                        nonRunningMs, request.remainingWorkMs());
            }
        }

        this.knownNonRunningWorkMs = nonRunningMs;
        this.runningWorkMs = runningCount == 0
                ? EMPTY_LONGS : new long[runningCount];
        this.requestIds = requestIdCount == 0
                ? EMPTY_STRINGS : new String[requestIdCount];
        int runningIndex = 0;
        int requestIdIndex = 0;
        for (RequestWork request : this.requests) {
            requestIds[requestIdIndex++] = request.requestId();
            if (request.phase() == Phase.ENGINE_RUNNING) {
                runningWorkMs[runningIndex++] = request.remainingWorkMs();
            }
        }
        Arrays.sort(requestIds);
    }

    public long createdAtMs() {
        return createdAtMs;
    }

    public List<RequestWork> requests() {
        return requests;
    }

    public long unknownRequestCount() {
        return unknownRequestCount;
    }

    /** Lifecycle phase visible to a estimate. Only ENGINE_RUNNING consumes time. */
    public enum Phase {
        COMMITTED,
        ENGINE_QUEUED,
        ENGINE_RUNNING
    }

    /** One individually delivered request, identified by request id. */
    public record RequestWork(String requestId,
                              Phase phase,
                              long remainingWorkMs) {

        public RequestWork {
            if (remainingWorkMs < 0L) {
                throw new IllegalArgumentException(
                        "remaining request work must be non-negative");
            }
        }
    }

    public boolean hasUnknownWork() {
        return unknownRequestCount > 0L;
    }

    public boolean containsRequest(String requestId) {
        return Arrays.binarySearch(requestIds, requestId) >= 0;
    }

    /**
     * Complete committed duration, absent when any work unit lacks a duration.
     */
    public OptionalLong totalRemainingWorkMs() {
        return hasUnknownWork()
                ? OptionalLong.empty()
                : OptionalLong.of(knownRemainingWorkMs());
    }

    /** Complete preceding work at a later clock; only observed running work consumes time. */
    public OptionalLong totalRemainingWorkMsAt(long observedAtMs) {
        return hasUnknownWork()
                ? OptionalLong.empty()
                : OptionalLong.of(knownRemainingWorkMsAt(observedAtMs));
    }

    /**
     * Sum of work units whose duration is known. Callers that require a complete
     * endpoint total must use {@link #totalRemainingWorkMs()}.
     */
    public long knownRemainingWorkMs() {
        long total = knownNonRunningWorkMs;
        for (long runningMs : runningWorkMs) {
            total = saturatedAdd(total, runningMs);
        }
        return total;
    }

    /** Known work rebased to a later planning clock without copying the snapshot. */
    public long knownRemainingWorkMsAt(long planningAtMs) {
        long elapsedMs = planningAtMs <= createdAtMs ? 0L : planningAtMs - createdAtMs;
        long total = knownNonRunningWorkMs;
        for (long runningMs : runningWorkMs) {
            long remaining = elapsedMs >= runningMs
                    ? 0L : runningMs - elapsedMs;
            total = saturatedAdd(total, remaining);
        }
        return total;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WorkView that)) {
            return false;
        }
        return createdAtMs == that.createdAtMs && unknownRequestCount == that.unknownRequestCount
                && requests.equals(that.requests);
    }

    @Override
    public int hashCode() {
        return Objects.hash(createdAtMs, requests, unknownRequestCount);
    }

    @Override
    public String toString() {
        return "WorkView[createdAtMs=" + createdAtMs + ", requests=" + requests
                + ", unknownRequestCount=" + unknownRequestCount + ']';
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
