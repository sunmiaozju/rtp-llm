package org.flexlb.balance.endpoint;

import lombok.Getter;
import org.flexlb.balance.prediction.FormulaPredictor;
import org.flexlb.balance.prediction.LearningPredictor;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.scheduler.EndpointEventHandler;
import org.flexlb.balance.schedulev2.prefill.PrefillQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.service.monitor.RequestSchedulerReporter;

import java.util.OptionalLong;
import java.util.function.Supplier;

public class PrefillEndpoint extends WorkerEndpoint {

    @Getter
    private final PrefillTimePredictor predictor;
    private final PrefillQueue prefillQueue;
    private final PrefillQueueRegistry prefillQueueRegistry;
    private final EndpointEventHandler endpointEvents;

    PrefillEndpoint(WorkerStatus status,
                    Supplier<FlexlbConfig> configSupplier,
                    EndpointEventHandler endpointEvents,
                    PrefillQueueRegistry prefillQueueRegistry,
                    RequestSchedulerReporter requestReporter) {
        super(status);
        this.endpointEvents = endpointEvents;
        this.prefillQueueRegistry = prefillQueueRegistry;
        FlexlbConfig startupConfig = configSupplier.get();
        this.predictor = createPredictor(startupConfig);
        this.prefillQueue = new PrefillQueue(this, startupConfig.isPriorityOrdering(), requestReporter);
    }

    /**
     * 由这个节点创建并持有的 Prefill 队列。
     */
    public PrefillQueue prefillQueue() {
        return prefillQueue;
    }

    @Override
    void onPublished() {
        prefillQueue.initializeFromCachedStatus();
        prefillQueueRegistry.start(this);
    }

    @Override
    protected void closeEndpoint() {
        prefillQueue.removeDispatchedOnRetirement();
        prefillQueueRegistry.retire(this);
    }

    private static PrefillTimePredictor createPredictor(FlexlbConfig config) {
        RoutingConfig.ExecutionTimeEstimatorConfig estimator = config.getRouter()
                .getRoles().getPrefill().getExecutionTimeEstimator();
        if (estimator.getType() == RoutingConfig.EstimatorType.LEARNING) {
            return new LearningPredictor();
        }
        return new FormulaPredictor(estimator.getExpression());
    }

    public boolean isRetired() {
        return isGenerationRetiringOrRetired();
    }

    @Override
    public Runnable applyPreparedStatus(WorkerStatus ws, WorkerStatus.PreparedStatus prepared) {
        try {
            super.applyPreparedStatus(ws, prepared);
        } catch (RuntimeException | Error failure) {
            if (ws == getStatus()) {
                beginRetirement();
            }
            throw failure;
        }
        return () -> endpointEvents.onPrefillStatus(this, prepared.observation());
    }

    @Override
    public Runnable observeStatusHeartbeat(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        super.observeStatusHeartbeat(ws, observation);
        return () -> endpointEvents.onPrefillStatus(this, observation);
    }

    @Override
    public OptionalLong getLoadMetric() {
        return prefillQueue.dispatchedWorkMs();
    }
}
