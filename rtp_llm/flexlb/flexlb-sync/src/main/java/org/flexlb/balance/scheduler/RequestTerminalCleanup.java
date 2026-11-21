package org.flexlb.balance.scheduler;

import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.util.Logger;

/** Executes an already-owned terminal action, then commits its terminal record and publishes. */
final class RequestTerminalCleanup {

    private final ExpirationTimer expirationTimer;
    private final RequestSchedulerReporter reporter;

    RequestTerminalCleanup(ExpirationTimer timer, RequestSchedulerReporter reporter) {
        this.expirationTimer = timer;
        this.reporter = reporter;
    }

    // ── 共同收尾：清理、提交记录、发布结果 ──

    void submitTerminal(TerminalAction action) {
        if (action == null) {
            return;
        }
        RequestCompletionPublisher.PublicationPermit permit = finishTerminal(action);
        if (permit != null && action.response() != null) {
            action.slot().submitTerminalResponse(permit, action.response());
        }
    }

    RequestCompletionPublisher.PublicationPermit finishTerminal(
            TerminalAction action) {
        RequestSlot entry = action.slot();
        entry.requireCleanupOwner(action);
        Throwable cleanupFailure = null;
        cleanupFailure = runStep(
                cleanupFailure,
                () -> action.terminalResources().release(expirationTimer));

        TerminationResult result = entry.commitTerminalRecord(action);
        Throwable terminalFailure = result.transitionFailure() == null
                ? cleanupFailure
                : appendFailure(cleanupFailure, result.transitionFailure());
        if (terminalFailure != null) {
            reporter.reportLifecycleFailure("terminal_cleanup");
            Logger.error("Terminal cleanup isolated after canonical claim: request_id={}",
                    entry.requestId(), terminalFailure);
        }
        return result.terminal() == null
                ? null : result.publication();
    }

    static Throwable runStep(Throwable first, Runnable leaf) {
        if (leaf == null) {
            return first;
        }
        try {
            leaf.run();
            return first;
        } catch (Throwable failure) {
            return appendFailure(first, failure);
        }
    }

    static Throwable appendFailure(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }

    static void rethrowCleanup(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "request slot cleanup failed", failure);
        }
    }

}
