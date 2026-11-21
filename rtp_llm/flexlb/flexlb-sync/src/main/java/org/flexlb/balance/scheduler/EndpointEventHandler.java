package org.flexlb.balance.scheduler;

import lombok.Getter;
import org.flexlb.balance.endpoint.EncoderEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.schedulev2.RequestStatusTimeout;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 节点完成状态提交后，向对应的调度链路交付同一次 WorkerStatus。
 * Prefill 队列和 Encoder 请求表分别消费各自角色的状态。
 */
@Component
public final class EndpointEventHandler {

    private final RequestRegistry scheduler;
    private final PrefillQueueRegistry prefillQueueRegistry;
    @Getter
    private final ConfigService configService;
    @Getter
    private final RequestSchedulerReporter requestReporter;

    public EndpointEventHandler(RequestRegistry scheduler,
                                PrefillQueueRegistry prefillQueueRegistry,
                                ConfigService configService,
                                RequestSchedulerReporter requestReporter) {
        this.scheduler = scheduler;
        this.prefillQueueRegistry = prefillQueueRegistry;
        this.configService = configService;
        this.requestReporter = requestReporter;
    }

    public void onPrefillStatus(PrefillEndpoint endpoint, WorkerStatus.StatusObservation status) {
        var queue = prefillQueueRegistry.find(endpoint);
        if (queue != null) {
            queue.handlePrefillStatus(status, RequestStatusTimeout.timeoutMs(configService));
        }
    }

    public void onEncoderStatus(EncoderEndpoint source,
                                List<WorkerStatus.TaskObservation> running,
                                List<WorkerStatus.TaskObservation> finished) {
        for (WorkerStatus.TaskObservation task : running) {
            try {
                scheduler.processEncoderStatus(source, task, false);
            } catch (Throwable failure) {
                logErrorNoFail("Encoder status fact handling isolated: request_id={}",
                        task.requestId(), failure);
            }
        }
        for (WorkerStatus.TaskObservation task : finished) {
            try {
                scheduler.processEncoderStatus(source, task, true);
            } catch (Throwable failure) {
                logErrorNoFail("Encoder finished fact handling isolated: request_id={}",
                        task.requestId(), failure);
            }
        }
        onEncoderCapacityChanged();
    }

    public void onEncoderCapacityChanged() {
        try {
            scheduler.encoderCapacityChanged();
        } catch (Throwable failure) {
            logErrorNoFail("Encoder capacity notification isolated", failure);
        }
    }

    private static void logErrorNoFail(String format, Object... arguments) {
        try {
            Logger.error(format, arguments);
        } catch (Throwable ignoredDiagnosticFailure) {
            // Continue handling the remaining exact facts.
        }
    }

}
