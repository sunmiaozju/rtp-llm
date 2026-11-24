package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;

/** Owns scheduler maintenance, metrics traversal, and ordered shutdown. */
@Component
final class SchedulerRuntime {

    private static final long EXPIRATION_MAINTENANCE_INTERVAL_MS = 60_000L;

    private final RequestRegistry requests;
    private final EndpointRegistry endpoints;
    private final BatchSchedulerReporter reporter;
    private final ConfigService configService;
    private final QueueCapacity queueCapacity;
    private final Runnable closePlacement;
    private final List<RoleType> queueRoles;

    SchedulerRuntime(RequestRegistry requests,
                     EndpointRegistry endpoints,
                     BatchSchedulerReporter reporter,
                     ConfigService configService,
                     QueueCapacity queueCapacity,
                     ModelMetaConfig modelMetaConfig) {
        this(requests, endpoints, reporter,
                configService, queueCapacity, modelMetaConfig, () -> { });
    }

    @Autowired
    SchedulerRuntime(RequestRegistry requests,
                     EndpointRegistry endpoints,
                     BatchSchedulerReporter reporter,
                     RequestScheduler scheduler,
                     ConfigService configService,
                     QueueCapacity queueCapacity,
                     ModelMetaConfig modelMetaConfig) {
        this(requests, endpoints, reporter, configService, queueCapacity, modelMetaConfig,
                scheduler::closePlacement);
    }

    private SchedulerRuntime(RequestRegistry requests,
                             EndpointRegistry endpoints,
                             BatchSchedulerReporter reporter,
                             ConfigService configService,
                             QueueCapacity queueCapacity,
                             ModelMetaConfig modelMetaConfig,
                             Runnable closePlacement) {
        this.queueRoles = modelMetaConfig.getRequiredRoles().stream()
                .filter(role -> role == RoleType.PREFILL || role == RoleType.PDFUSION)
                .toList();
        this.requests = requests;
        this.endpoints = endpoints;
        this.reporter = reporter;
        this.configService = configService;
        this.queueCapacity = queueCapacity;
        this.closePlacement = closePlacement;
    }

    @Scheduled(fixedRate = EXPIRATION_MAINTENANCE_INTERVAL_MS)
    void maintainExpiration() {
        requests.maintainExpiration(endpoints::purgeFinishedDecodeRequests);
    }

    @Scheduled(fixedRateString = "${report.interval.ms:2000}")
    void report() {
        if (requests.isShuttingDown()) {
            return;
        }
        reportSchedulerInflight();
        reportSharedQueueCapacity();
        reportDecodeEndpoints();
    }

    private void reportSchedulerInflight() {
        try {
            RequestRegistry.TrackedRequestStats stats = requests.trackedRequestStats();
            reporter.reportTrackedRequestCount(stats.generationRequests());
            reporter.reportTrackedRequestCount(RoleType.ENCODER, stats.encoderRequests());
        } catch (RuntimeException failure) {
            warnIsolated(
                    "Failed to report scheduler inflight metrics", failure);
        }
    }

    private void reportSharedQueueCapacity() {
        FlexlbConfig config;
        try {
            config = configService.loadBalanceConfig();
        } catch (RuntimeException failure) {
            warnIsolated("Failed to read shared queue configuration for metrics", failure);
            return;
        }
        if (config == null) {
            return;
        }
        try {
            reporter.reportGlobalQueueLimit(config.getScheduler().getMaxQueuedRequests());
            reporter.reportTotalQueueWaiting(queueCapacity.globalQueueAndEngineSubQueueCount());
        } catch (RuntimeException failure) {
            warnIsolated("Failed to report total queued requests", failure);
        }
        for (RoleType role : queueRoles) {
            try {
                int logicalEndpointCount = endpoints.getEndpointCount(role);
                reporter.reportSharedSubQueue(role.name(), queueCapacity.engineSubQueueWaiting(role),
                        queueCapacity.engineSubQueueLimit(config, logicalEndpointCount), logicalEndpointCount);
            } catch (RuntimeException failure) {
                warnIsolated("Failed to report shared queue capacity for " + role, failure);
            }
        }
    }

    private void reportDecodeEndpoints() {
        for (WorkerEndpoint worker : EndpointRegistry.getEndpointsByRole(RoleType.DECODE).values()) {
            if (!(worker instanceof DecodeEndpoint endpoint)) {
                continue;
            }
            try {
                endpoint.reportBatchMetrics(reporter);
            } catch (RuntimeException failure) {
                warnIsolated("Failed to report Decode endpoint metrics: endpoint=" + endpoint.ipPort(), failure);
            }
        }
    }

    private static void warnIsolated(
            String message, RuntimeException failure) {
        try {
            Logger.warn(message, failure);
        } catch (Throwable ignored) {
            // Telemetry must never couple otherwise independent metric leaves.
        }
    }

    @PreDestroy
    void shutdown() {
        Throwable failure = null;
        boolean ownsShutdown;
        try {
            closePlacement.run();
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            ownsShutdown = requests.closeAdmissionAndAwaitMutations();
        } catch (Throwable closeFailure) {
            failure = append(failure, closeFailure);
            ownsShutdown = true;
        }
        if (!ownsShutdown) {
            return;
        }
        try {
            try {
                requests.closeOutstandingAndTerminalize();
            } catch (Throwable closeFailure) {
                failure = append(failure, closeFailure);
            }
            try {
                requests.closeExpiration();
            } catch (Throwable closeFailure) {
                failure = append(failure, closeFailure);
            }
            try {
                endpoints.close();
            } catch (Throwable closeFailure) {
                failure = append(failure, closeFailure);
            }
        } finally {
            try {
                requests.closePublisher();
            } catch (Throwable closeFailure) {
                failure = append(failure, closeFailure);
            }
        }
        rethrow(failure);
    }

    private static Throwable append(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            try {
                first.addSuppressed(next);
            } catch (Throwable ignored) {
                // Preserve the primary failure and continue shutdown.
            }
        }
        return first;
    }

    private static void rethrow(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("Scheduler shutdown failed", failure);
    }
}
