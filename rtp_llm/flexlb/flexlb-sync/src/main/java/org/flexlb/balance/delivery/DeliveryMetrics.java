package org.flexlb.balance.delivery;

import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Logger;

import java.util.List;

/**
 * No-throw metrics emitted after a delivery decision has committed.
 */
public final class DeliveryMetrics {

    private final BatchSchedulerReporter reporter;

    public DeliveryMetrics(BatchSchedulerReporter reporter) {
        this.reporter = reporter;
    }

    public void routesDelivered(int remainingQueueDepth, List<ScheduledRequest> exactItems) {
        try {
            if (exactItems.isEmpty()) {
                return;
            }
            ScheduledRequest head = exactItems.get(0);
            String engineIp = prefillIp(head);
            String role = prefillRole(head);
            var decisionGroup = head.ctx().getDecisionGroup();
            if (decisionGroup != null) {
                reporter.reportDispatchReason(role, engineIp, decisionGroup.reason());
                reporter.reportBatchSize(role, engineIp, decisionGroup.reason(), exactItems.size());
            }
            reporter.reportBatcherQueueSize(
                    role, engineIp,
                    remainingQueueDepth);
            long nowMs = System.currentTimeMillis();
            for (ScheduledRequest item : exactItems) {
                reporter.reportBatchWaitTimeMs(
                        role,
                        engineIp,
                        Math.max(0L, nowMs - item.enqueuedAtMs()),
                        item.priority());
            }
        } catch (Throwable failure) {
            Logger.warn("Route delivery telemetry isolated", failure);
        }
    }

    private static String prefillRole(ScheduledRequest item) {
        var status = item.prefillEp().getStatus();
        RoleType role = status == null ? null : status.getRole();
        return role == null ? RoleType.PREFILL.name() : role.name();
    }

    private static String prefillIp(ScheduledRequest item) {
        var status = item.prefillEp().getStatus();
        return status == null ? "" : status.getMetricIpPort();
    }

}
