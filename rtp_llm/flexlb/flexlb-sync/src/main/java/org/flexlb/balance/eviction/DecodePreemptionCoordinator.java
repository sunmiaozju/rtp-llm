package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.DecodeEndpoint.DecodeRequestView;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Executes one Engine-Cancel preemption transaction.
 *
 * <p>The scheduler supplies a pure plan and consumes one result.  This class
 * owns the two-phase protocol, token fencing and exactly-once child settlement.
 * Engine acknowledgement is only control evidence; the canonical victim
 * terminal transaction may complete before or after that acknowledgement.</p>
 */
@Component
public final class DecodePreemptionCoordinator {

    public record PreemptionResult(
            boolean committed, boolean controlFailure, String detail) {
        public PreemptionResult {
            if (committed && controlFailure) {
                throw new IllegalArgumentException(
                        "committed preemption cannot be a control failure");
            }
        }
    }

    public record PreemptionCommand(DecodeEndpoint endpoint,
                             String incomingRequestId,
                             long incomingKvTokens,
                             long incomingKvBudgetTokens,
                             int incomingPriority,
                             DecodeEndpoint.CapacityLimit capacity,
                             List<DecodeRequestView> victims,
                             long cancelAckTimeoutMs,
                             long preemptionTimeoutMs,
                             BooleanSupplier admissionOpen,
                             String detail) {
        public PreemptionCommand {
            if (endpoint == null || victims == null || victims.isEmpty()) {
                throw new IllegalArgumentException("endpoint and victims are required");
            }
            if (incomingRequestId == null || incomingRequestId.isBlank()) {
                throw new IllegalArgumentException(
                        "incoming request id must not be blank");
            }
            if (capacity == null) {
                throw new IllegalArgumentException("capacity policy is required");
            }
            Set<String> victimIds = new LinkedHashSet<>();
            for (DecodeRequestView victim : victims) {
                if (victim.requestId() == null || victim.requestId().isBlank()
                        || victim.reservationToken() <= 0L) {
                    throw new IllegalArgumentException(
                            "victim requestId must not be blank and reservation token must be positive");
                }
                if (victim.phase() == null
                        || !victim.phase().requiresEngineCancel()) {
                    throw new IllegalArgumentException(
                            "coordinator accepts only Engine-Cancel victims");
                }
                if (!victimIds.add(victim.requestId())) {
                    throw new IllegalArgumentException(
                            "duplicate victim " + victim.requestId());
                }
            }
            if (admissionOpen == null) {
                throw new IllegalArgumentException("admission gate is required");
            }
        }
    }

    private final RequestSchedulerReporter reporter;
    private final AtomicLong tokenSequence = new AtomicLong(1);

    public DecodePreemptionCoordinator(RequestSchedulerReporter reporter) {
        this.reporter = reporter;
    }

    public CompletableFuture<PreemptionResult> prepareReturnedPreemption(
            PreemptionCommand command) {
        long token = nextToken();
        long generation = command.endpoint().getStatus().getGenerationId();
        List<DecodeEndpoint.ReservationHandle> victims = command.victims().stream()
                .map(victim -> new DecodeEndpoint.ReservationHandle(
                        generation, victim.requestId(), victim.reservationToken()))
                .toList();
        DecodeEndpoint.PreemptionBeginResult begin = command.endpoint()
                .beginReturnedPreemption(
                        token,
                        victims,
                        command.incomingRequestId(),
                        command.incomingKvTokens(),
                        command.incomingKvBudgetTokens(),
                        command.incomingPriority(),
                        command.capacity());
        reportTargetValidationResult("return", begin);
        return CompletableFuture.completedFuture(new PreemptionResult(
                begin == DecodeEndpoint.PreemptionBeginResult.SUCCESS,
                begin == DecodeEndpoint.PreemptionBeginResult.ENDPOINT_RETIRED,
                "return_" + begin.name().toLowerCase()));
    }

    private void reportTargetValidationResult(
            String mode, DecodeEndpoint.PreemptionBeginResult result) {
        String reason = switch (result) {
            case VICTIM_GONE -> "victim_state_changed";
            case VICTIM_ALREADY_CLAIMED -> "victim_already_claimed";
            case INVALID_PRIORITY -> "priority_not_preemptible";
            default -> null;
        };
        if (reason != null) {
            reportTargetValidationFailure(mode, reason);
        }
    }

    private void reportTargetValidationFailure(String mode, String reason) {
        try {
            reporter.reportPreemptionTargetInvalid(mode, reason);
        } catch (RuntimeException metricFailure) {
            Logger.warn("Failed to report preemption target validation: mode={} reason={}",
                    mode, reason, metricFailure);
        }
    }

    private long nextToken() {
        long token = tokenSequence.getAndIncrement();
        if (token <= 0) {
            throw new IllegalStateException("preemption attempt token exhausted");
        }
        return token;
    }
}
