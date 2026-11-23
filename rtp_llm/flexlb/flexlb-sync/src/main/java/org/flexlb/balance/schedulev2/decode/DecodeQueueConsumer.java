package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.eviction.DecodeEndpointSnapshot;
import org.flexlb.balance.eviction.DecodeEvictionProposal;
import org.flexlb.balance.eviction.DecodePreemptionCoordinator;
import org.flexlb.balance.eviction.EvictionPlanner;
import org.flexlb.balance.schedulev2.PrefillPublishCheck;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.request.DecodeChoice;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.SelectedDecode;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.DecodeRequirement;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.DecodeMode;
import org.flexlb.service.monitor.V2QueueReporter;
import org.flexlb.util.CommonUtils;
import org.flexlb.util.Logger;
import org.flexlb.util.PriorityNormalizer;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一个 Decode 队列消费者，一次只持有一个已取得 Prefill 名额的请求。
 * 该请求的 Decode 选择、预占、permit、发布和归还都由本线程执行；失败后只归还 Decode 预占，
 * 保留 Prefill 名额等待集群容量变化，再重新选择 Decode。发布成功后由引擎上报回收两端资源。
 * <p>
 * 当前部署只有一个 Decode group，所有请求共享同一组 Decode 节点。消费者可持有请求等待该组容量恢复，
 * 不需要为其他 group 留出线程。若部署多个 group，须避免一个 group 占满消费者，阻塞其他 group 的请求。
 */
public final class DecodeQueueConsumer implements Runnable {

    private final DecodeQueue decodeQueue;
    private final RequestCleanup cleanup;
    private final DecodeSelector decodeSelector;
    private final DecodePreemptionCoordinator preemptionCoordinator;
    private final PrefillPublishCheck prefillCheck;
    private final V2QueueReporter queueReporter;
    private final Runnable capacityWake; // 全体消费者共享同一实例，节点侧 Set 自动去重
    private final AtomicBoolean running = new AtomicBoolean(true);

    public DecodeQueueConsumer(DecodeQueue decodeQueue,
                               RequestCleanup cleanup,
                               DecodeSelector decodeSelector,
                               DecodePreemptionCoordinator preemptionCoordinator,
                               PrefillPublishCheck prefillCheck,
                               V2QueueReporter queueReporter,
                               Runnable capacityWake) {
        this.decodeQueue = decodeQueue;
        this.cleanup = cleanup;
        this.decodeSelector = decodeSelector;
        this.preemptionCoordinator = preemptionCoordinator;
        this.prefillCheck = prefillCheck;
        this.queueReporter = queueReporter;
        this.capacityWake = capacityWake;
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                try {
                    Request request = decodeQueue.take(running::get);
                    if (request != null) {
                        process(request);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    running.set(false);
                } catch (Throwable failure) {
                    Logger.error("V2 decode queue consumer failed", failure.getMessage());
                }
            }
        } finally {
            running.set(false);
        }
    }

    public void close() {
        running.set(false);
        decodeQueue.wake();
    }

    private void process(Request request) throws InterruptedException {
        boolean returnToMain = false;
        try {
            String group = request.selectedPrefill().serverStatus().getGroup();
            while (running.get()) {
                if (request.stage() != Stage.DECODE_WAITING) {
                    break;
                }
                if (!prefillCheck.isAvailable(request)) { // 等待期间所选 Prefill 也可能失效
                    returnToMain = true;
                    break;
                }
                // 尝试前记下通知版本；尝试期间的容量变化不应再睡眠。
                long version = decodeQueue.changeVersion();
                if (selectAndPublish(request, group)) { // 发布成功，本次处理完成
                    break;
                }
                // 恢复等待阶段，等待容量变化后重试，避免没有可用 Decode 时忙循环。
                request.moveTo(Stage.DECODE_RESERVED, Stage.DECODE_WAITING);
                decodeQueue.awaitChange(version);
            }
        } catch (InterruptedException interrupted) {
            throw interrupted; // 交给 run() 停线程，不误杀请求
        } catch (Throwable failure) {
            request.fail(StrategyErrorType.DISPATCH_FAILED, "decode selection failed: " + failure.getMessage());
            Logger.error("V2 decode selection failed", failure);
        } finally {
            // 并发取消、失败、退回主队列等路径的兜底清理；已发布的资源由引擎上报回收。
            if (request.stage() != Stage.PUBLISHED) {
                releaseDecode(request);
                prefillCheck.release(request);
                // 归还完本线程取得的资源后才退回主队列，不能在新一轮选路开始后再清理旧资源。
                if (returnToMain) {
                    request.moveTo(Stage.DECODE_RESERVED, Stage.DECODE_WAITING);
                    cleanup.returnToMain(request, Stage.DECODE_WAITING);
                }
            }
        }
    }

    private boolean selectAndPublish(Request request, String group) {
        SelectedDecode selected = decodeSelector.select(request.ctx(), group);
        if (selected == null) {
            queueReporter.blocked(request, Stage.DECODE_WAITING, QueueBlockReason.NO_AVAILABLE_DECODE, "all");
            return false;
        }
        return selected.selectedAsEmpty()
                ? placeOnEmptyDecode(request, selected.endpoint())
                : reserve(request, selected.endpoint());
    }

    private boolean reserve(Request request, DecodeEndpoint endpoint) {
        if (endpoint.isRetired()) {
            return publish(request, new DecodeChoice(endpoint, null), null, Stage.DECODE_WAITING);
        }
        endpoint.addEngineDispatchCapacityListener(capacityWake);
        List<DecodeEndpoint.DecodeRequestView> preemptedRequests = new ArrayList<>();
        DecodeRequirement requirement = request.ctx().getDecodeRequirement();
        DecodeEndpoint.DispatchReservation reserved =
                endpoint.reserveForDispatch(request.requestId(), requirement.inputKvTokens(),
                        requirement.inputAndMaxOutputKvTokens(), requirement.priority(), requirement.capacity());
        if (reserved == null) {
            reserved = preempt(request, endpoint, requirement, preemptedRequests);
            if (reserved == null) {
                if (endpoint.isRetired()) {
                    return publish(request, new DecodeChoice(endpoint, null), null, Stage.DECODE_WAITING);
                }
                reportCapacityBlock(request, endpoint, requirement);
                return false;
            }
        }
        DecodeChoice selectedDecode = new DecodeChoice(endpoint, reserved.reservation(), preemptedRequests);
        request.setDecodeChoice(selectedDecode);

        // 先记录预占再推进阶段；取消先赢 CAS 时直接归还刚取得的预占。
        if (!request.moveTo(Stage.DECODE_WAITING, Stage.DECODE_RESERVED)) {
            releaseDecode(request);
            return false;
        }

        return publish(request, selectedDecode, reserved.permit(), Stage.DECODE_RESERVED);
    }

    /**
     * 空机放行不取 permit，预占仍记入节点账本；空机确认和预占在节点锁内完成。
     */
    private boolean placeOnEmptyDecode(Request request, DecodeEndpoint endpoint) {
        if (endpoint.isRetired()) {
            return false;
        }
        DecodeRequirement requirement = request.ctx().getDecodeRequirement();
        var reservation = endpoint.reserveIfEmpty(request.requestId(), requirement.inputKvTokens(),
                requirement.inputAndMaxOutputKvTokens(), requirement.priority());
        if (reservation == null) {
            return false;
        }
        var choice = new DecodeChoice(endpoint, reservation);
        request.setDecodeChoice(choice);
        return publish(request, choice, null, Stage.DECODE_WAITING);
    }

    private boolean publish(Request request,
                            DecodeChoice choice,
                            DecodeEndpoint.EngineDispatchPermit permit,
                            Stage expected) {

        if (permit != null) {
            try {
                // 即使摘流清掉了这个名额，也返回已经选中的地址。
                if (permit.dispatch() == DecodeEndpoint.EngineDispatchPermitTransferStatus.OWNERSHIP_LOST) {
                    permit.release();
                    releaseDecode(request);
                    return false;
                }
            } catch (RuntimeException failure) {
                permit.release();
                releaseDecode(request);
                return false;
            }
        }

        // 发布与取消共用阶段 CAS；只有发布成功才保留资源，并上报实际完成的抢占。
        ServerStatus decodeStatus = decodeServerStatus(choice.endpoint(), request.requestId());
        decodeStatus.setPreemptRequestIds(
                choice.preemptedRequests().stream().map(DecodeEndpoint.DecodeRequestView::requestId).toList());
        boolean published = request.publish(expected, List.of(request.selectedPrefill().serverStatus(), decodeStatus));

        if (published) {
            if (choice.reservation() != null) {
                choice.endpoint().registerPublished(choice.reservation(), request.publishedAtMs());
            }
            cleanup.releasePublished(request);
            for (DecodeEndpoint.DecodeRequestView victim : choice.preemptedRequests()) {
                queueReporter.decodePreempted(request.priority(), victim.priority(), victim.phase(),
                        choice.endpoint().ipPort());
            }
            return true;
        }
        releaseDecode(request);
        return false;
    }

    /**
     * 从所选 Decode 的当前状态生成返回给调用方的地址信息。
     */
    private static ServerStatus decodeServerStatus(DecodeEndpoint endpoint, String requestId) {
        WorkerStatus worker = endpoint.getStatus();
        WorkerStatus.TopologySnapshot topology = worker.topologySnapshot();
        ServerStatus serverStatus = new ServerStatus();
        serverStatus.setSuccess(true);
        serverStatus.setRole(RoleType.DECODE);
        serverStatus.setServerIp(topology.ip());
        serverStatus.setHttpPort(topology.port());
        serverStatus.setGrpcPort(CommonUtils.toGrpcPort(topology.port()));
        serverStatus.setDpRank(worker.committedEngineObservation().dpRank());
        serverStatus.setGroup(topology.group());
        serverStatus.setRequestId(requestId);
        serverStatus.setSelectedEngineIndex(topology.engineIndex(), topology.multiEngineNum());
        return serverStatus;
    }

    /**
     * 结果未发布时按 NOT_SENT 归还；RETURN 抢占尚未下发时改用 LOCAL_ROLLBACK 撤销抢占。
     */
    private void releaseDecode(Request request) {
        DecodeChoice choice = request.takeDecodeChoice();
        try {
            if (choice != null) {
                DecodeEndpoint.ReservationReleaseResult result =
                        choice.endpoint().release(choice.reservation(), DecodeEndpoint.ReleaseReason.NOT_SENT);
                if (result == DecodeEndpoint.ReservationReleaseResult.CONFLICT) { // RETURN 抢占记录仍阻止普通归还
                    choice.endpoint().release(choice.reservation(), DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
                }
            }
        } catch (RuntimeException failure) {
            Logger.error("V2 decode reservation release failed: request_id={}", request.requestId(), failure);
        }
    }

    /**
     * 只抢占这台 Decode 上已发给引擎的低优先级请求，RETURN 模式由调用方按响应 ID 取消，不发 cancel RPC。
     */
    private DecodeEndpoint.DispatchReservation preempt(Request request,
                                                       DecodeEndpoint endpoint,
                                                       DecodeRequirement requirement,
                                                       List<DecodeEndpoint.DecodeRequestView> preemptedRequests) {
        PreemptionConfig configured =
                request.ctx().getConfig().queueScheduler().getOrdering().preemptionPolicy().orElse(null);
        if (configured == null || !returnPreemptionAllowed(requirement, configured)) {
            return null;
        }
        DecodeEndpointSnapshot snapshot = DecodeEndpointSnapshot.capture(endpoint, requirement.capacity());
        DecodeEvictionProposal proposal = EvictionPlanner.planDecode(requirement.priority(),
                requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens(), List.of(snapshot),
                V2_RETURN_PREEMPTION, new HashMap<>());
        if (proposal == null || !proposal.hasEngineFacingVictims()) {
            return null;
        }
        DecodePreemptionCoordinator.PreemptionCommand command = new DecodePreemptionCoordinator.PreemptionCommand(
                endpoint, request.requestId(), requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens(),
                requirement.priority(), requirement.capacity(), proposal.victims());

        DecodeEndpoint.PreemptionBeginResult result = preemptionCoordinator.prepareReturnedPreemption(command);

        if (result != DecodeEndpoint.PreemptionBeginResult.SUCCESS) {
            Logger.debug("V2 decode preemption reservation failed: request_id={} result={}", request.requestId(),
                    result);
            return null;
        }
        DecodeEndpoint.ReservationHandle reservation = endpoint.reservationHandle(request.requestId());
        if (reservation == null) {
            return null;
        }
        // RETURN 抢占沿用现有事务，先标记 queued 再取得 permit；抢占名额不做普通容量复查。
        if (!endpoint.markQueued(reservation)) {
            endpoint.release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            return null;
        }

        DecodeEndpoint.EngineDispatchPermitAcquisition acquisition =
                endpoint.acquireDispatchPermit(reservation, requirement.capacity());

        if (acquisition.permit() == null) {
            endpoint.release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            return null;
        }
        preemptedRequests.addAll(proposal.victims());

        return new DecodeEndpoint.DispatchReservation(reservation, acquisition.permit());
    }

    private void reportCapacityBlock(Request request, DecodeEndpoint endpoint, DecodeRequirement requirement) {
        // 失败后再读一次容量，避免按旧快照误报不足
        var view = endpoint.routingView();
        var deficit = requirement.capacity()
                .evaluate(view.placementUsage(), requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens());
        queueReporter.blocked(request, Stage.DECODE_WAITING, QueueBlockReason.decodeCapacity(deficit),
                endpoint.ipPort());
    }

    /**
     * V2 抢占只允许引擎侧请求且只发 RETURN 指令；规划器只读这个常量，不在请求间共享可变状态。
     */
    private static final PreemptionConfig V2_RETURN_PREEMPTION = v2ReturnPreemption();

    private static PreemptionConfig v2ReturnPreemption() {
        PreemptionConfig policy = new PreemptionConfig();
        policy.setAllowedVictimStages(EnumSet.of(VictimStage.DECODE_ENGINE_OWNED));
        return policy;
    }

    private static boolean returnPreemptionAllowed(DecodeRequirement requirement, PreemptionConfig configured) {
        return requirement.mode() == DecodeMode.PREEMPT_AT_PLACEMENT
                && PriorityNormalizer.hasPriority(requirement.priority()) && configured != null
                && configured.allows(VictimStage.DECODE_ENGINE_OWNED);
    }
}
