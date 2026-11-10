package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.PlacementResult.Status;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.eviction.DecodeEndpointSnapshot;
import org.flexlb.balance.eviction.DecodeEvictionProposal;
import org.flexlb.balance.eviction.DecodePreemptionCoordinator;
import org.flexlb.balance.eviction.EvictionPlanner;
import org.flexlb.balance.scheduler.decode.DecodeMode;
import org.flexlb.balance.scheduler.decode.DecodeRequirement;
import org.flexlb.balance.schedulev2.PrefillPublishCheck;
import org.flexlb.balance.schedulev2.QueueBlockReason;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.request.DecodeChoice;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.config.ConfigService;
import org.flexlb.config.EngineCancellationConfig;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
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
 */
public final class DecodeQueueConsumer implements Runnable {

    /**
     * 一次 Decode 选择及发布尝试的明确结果；返回 null 表示本次未发布，可重试。
     */
    private enum AttemptResult {
        PUBLISHED, RETURN_TO_MAIN // Prefill 不可用，归还资源后退回主队列重选 Prefill
    }

    private final DecodeQueue decodeQueue;
    private final RequestCleanup cleanup;
    private final DecodeSelector decodeSelector;
    private final DecodePreemptionCoordinator preemptionCoordinator;
    private final ConfigService configService;
    private final PrefillPublishCheck prefillCheck;
    private final DecodeReservations reservations;
    private final V2QueueReporter queueReporter;
    private final Runnable capacityWake; // 全体消费者共享同一实例，节点侧 Set 自动去重
    private final AtomicBoolean running = new AtomicBoolean(true);

    public DecodeQueueConsumer(DecodeQueue decodeQueue,
                               RequestCleanup cleanup,
                               DecodeSelector decodeSelector,
                               DecodePreemptionCoordinator preemptionCoordinator,
                               ConfigService configService,
                               PrefillPublishCheck prefillCheck,
                               DecodeReservations reservations,
                               V2QueueReporter queueReporter,
                               Runnable capacityWake) {
        this.decodeQueue = decodeQueue;
        this.cleanup = cleanup;
        this.decodeSelector = decodeSelector;
        this.preemptionCoordinator = preemptionCoordinator;
        this.configService = configService;
        this.prefillCheck = prefillCheck;
        this.reservations = reservations;
        this.queueReporter = queueReporter;
        this.capacityWake = capacityWake;
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                try {
                    Request request = decodeQueue.take(this::tryAcquirePrefill, running::get);
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

    private boolean tryAcquirePrefill(Request request) {
        try {
            // 结束路径已完成 future 和排队清理；此处尚未取得两端资源，直接丢弃即可。
            if (request.stage() != Stage.DECODE_WAITING) {
                return false;
            }
            if (!prefillCheck.isAvailable(request)) {
                cleanup.returnToMain(request, Stage.DECODE_WAITING);
                return false;
            }
            QueueBlockReason reason = prefillCheck.tryAcquire(request,
                    configService.loadBalanceConfig().getDispatcher().getMaxInflightPerPrefillWorker());
            if (reason == null) {
                return true;
            }
            queueReporter.blocked(request, request.stage(), reason, request.selectedPrefill().endpoint().ipPort());
        } catch (Throwable failure) { // 检查或上报异常时，不把请求留在无人处理的状态
            prefillCheck.release(request);
            request.fail(StrategyErrorType.DISPATCH_FAILED, "prefill capacity check failed: " + failure.getMessage());
            Logger.error("V2 decode prefill capacity check failed", failure);
        }
        return false;
    }

    private void process(Request request) throws InterruptedException {
        boolean returnToMain = false;
        DecodeRequirement requirement = DecodeRequirement.capture(request.ctx());
        String group = request.selectedPrefill().serverStatus().getGroup();
        try {
            while (running.get()) {
                if (request.stage() != Stage.DECODE_WAITING) {
                    break;
                }
                if (!prefillCheck.isAvailable(request)) { // 等待期间所选 Prefill 也可能失效
                    returnToMain = true;
                    break;
                }
                reservations.add(request.requestId());

                // 尝试前记下通知版本；尝试期间的容量变化不应再睡眠。
                long version = decodeQueue.changeVersion();
                AttemptResult attemptResult = selectAndPublish(request, group, requirement);
                if (attemptResult == AttemptResult.PUBLISHED) {
                    break;
                }
                if (attemptResult == AttemptResult.RETURN_TO_MAIN) {
                    returnToMain = true;
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

    private AttemptResult selectAndPublish(Request request, String group, DecodeRequirement requirement) {
        PlacementResult<SelectedRole, RoleType> result = decodeSelector.select(request.ctx(), requirement, group);

        switch (result.status()) {
            case SUCCESS -> {
                return reserve(request, result.value(), requirement);
            }
            case BLOCKED, REJECTED -> { // 无可用节点或请求超过所有节点额度，仍可尝试空机放行
                AttemptResult notRuningDecode = placeOnEmptyDecode(request, requirement, group);
                if (notRuningDecode != null) {
                    return notRuningDecode;
                }
                QueueBlockReason queueBlockReason = result.status() == Status.REJECTED
                        ? QueueBlockReason.REQUEST_EXCEEDS_KV_BUDGET
                        : QueueBlockReason.NO_AVAILABLE_DECODE;
                queueReporter.blocked(request, Stage.DECODE_WAITING, queueBlockReason, "all");
                return null;
            }
            default -> {
                return null;
            }
        }
    }

    private AttemptResult reserve(Request request, SelectedRole selected, DecodeRequirement requirement) {

        DecodeChoice selectedDecode;
        DecodeEndpoint.EngineDispatchPermit permit;
        List<DecodeEndpoint.DecodeRequestView> preemptedRequests = new ArrayList<>();

        try (selected; WorkerEndpoint.EndpointHold hold = selected.takeEndpointHold()) {
            DecodeEndpoint decodeEndpoint = (DecodeEndpoint) hold.endpoint();

            decodeEndpoint.addEngineDispatchCapacityListener(capacityWake);
            DecodeEndpoint.DispatchReservation reserved =
                    decodeEndpoint.reserveForDispatch(hold, request.requestId(), requirement.inputKvTokens(),
                            requirement.inputAndMaxOutputKvTokens(), requirement.priority(), requirement.capacity());

            if (reserved == null) { // 所选节点占不到资源，尝试低优抢占
                reserved = preempt(request, hold, requirement, selected.serverStatus(), preemptedRequests);
                if (reserved == null) {
                    reportCapacityBlock(request, decodeEndpoint, requirement);
                    return null;
                }
            }

            selectedDecode = new DecodeChoice(decodeEndpoint, reserved.reservation(), preemptedRequests);
            permit = reserved.permit();
            request.setDecodeChoice(selectedDecode);
        }

        // 先记录预占再推进阶段；取消先赢 CAS 时直接归还刚取得的预占。
        if (!request.moveTo(Stage.DECODE_WAITING, Stage.DECODE_RESERVED)) {
            releaseDecode(request);
            return null;
        }

        return publish(request, selectedDecode, permit, Stage.DECODE_RESERVED);
    }

    /**
     * 空机放行不取 permit，预占仍记入节点账本；空机确认和预占在节点锁内完成。
     */
    private AttemptResult placeOnEmptyDecode(Request request, DecodeRequirement requirement, String group) {
        DecodeChoice choice = reserveEmptyDecode(request, group, requirement);
        if (choice == null) {
            return null;
        }
        request.setDecodeChoice(choice);
        return publish(request, choice, null, Stage.DECODE_WAITING);
    }

    /**
     * 逐台尝试原子预占 group 里的空 Decode，返回预占；没有成功时返回 null。
     * 空的意思是引擎上报的 running 和 waiting 都是 0，且本地没有 reserve 或引擎已确认的请求；
     * 引擎上报只筛候选，最终空机判定和预占在节点锁内完成，避免多个消费者同时放行。
     */
    private DecodeChoice reserveEmptyDecode(Request request, String group, DecodeRequirement requirement) {
        for (WorkerEndpoint candidate : EndpointRegistry.getEndpointsByRole(RoleType.DECODE).values()) {
            if (!(candidate instanceof DecodeEndpoint endpoint) || endpoint.isRetired()) {
                continue;
            }
            WorkerStatus worker = endpoint.getStatus();
            if (group != null && !group.equals(worker.getGroup())) {
                continue;
            }
            WorkerStatus.EngineObservation engineObservation = worker.committedEngineObservation();
            if (engineObservation.runningQueryLen() != 0L || engineObservation.waitingQueryLen() != 0L) {
                continue;
            }
            DecodeEndpoint.ReservationHandle reservation = endpoint.reserveIfEmpty(request.requestId(),
                    requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens(), requirement.priority());
            if (reservation != null) {
                return new DecodeChoice(endpoint, reservation);
            }
        }
        return null;
    }

    private AttemptResult publish(Request request,
                                  DecodeChoice choice,
                                  DecodeEndpoint.EngineDispatchPermit permit,
                                  Stage expected) {

        try (WorkerEndpoint.EndpointHold hold = prefillCheck.hold(request)) {
            // 发布前再核对 Prefill 生命周期；失效时归还资源，退回主队列重选 Prefill。
            if (hold == null) {
                if (permit != null) {
                    permit.release();
                }
                releaseDecode(request);
                return AttemptResult.RETURN_TO_MAIN;
            }
            try { // 移交已取得的下发名额，不再次检查容量
                if (permit != null
                        && permit.dispatch() != DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED) {
                    permit.release();
                    releaseDecode(request);
                    return null;
                }
            } catch (RuntimeException failure) {
                permit.release();
                releaseDecode(request);
                return null;
            }

            // 发布与取消共用阶段 CAS；只有发布成功才保留资源，并上报实际完成的抢占。
            boolean published = request.publish(expected, List.of(request.selectedPrefill().serverStatus(),
                    decodeServerStatus(choice.endpoint(), request.requestId())));

            if (published) {
                choice.endpoint().registerPublished(choice.reservation(), request.publishedAtMs());
                cleanup.releasePublished(request);
                for (DecodeEndpoint.DecodeRequestView victim : choice.preemptedRequests()) {
                    queueReporter.decodePreempted(request.priority(), victim.priority(), victim.phase(),
                            choice.endpoint().ipPort());
                }
                return AttemptResult.PUBLISHED;
            }
            releaseDecode(request);
            return null;
        }
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
        } finally {
            reservations.remove(request.requestId());
        }
    }

    /**
     * 只抢占这台 Decode 上已发给引擎的低优先级请求，RETURN 模式由调用方按响应 ID 取消，不发 cancel RPC。
     */
    private DecodeEndpoint.DispatchReservation preempt(Request request,
                                                       WorkerEndpoint.EndpointHold hold,
                                                       DecodeRequirement requirement,
                                                       ServerStatus serverStatus,
                                                       List<DecodeEndpoint.DecodeRequestView> preemptedRequests) {
        PreemptionConfig configured =
                request.ctx().getConfig().queueScheduler().getOrdering().preemptionPolicy().orElse(null);
        if (configured == null || !returnPreemptionAllowed(requirement, configured)) {
            return null;
        }
        DecodeEndpoint endpoint = (DecodeEndpoint) hold.endpoint();
        DecodeEndpointSnapshot snapshot = DecodeEndpointSnapshot.capture(endpoint, requirement.capacity());
        DecodeEvictionProposal proposal = EvictionPlanner.planDecode(requirement.priority(),
                requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens(), List.of(snapshot),
                V2_RETURN_PREEMPTION, null, new HashMap<>());
        if (proposal == null || !proposal.requiresEngineCancel()) {
            return null;
        }
        DecodePreemptionCoordinator.PreemptionCommand command = new DecodePreemptionCoordinator.PreemptionCommand(
                endpoint, request.requestId(), requirement.inputKvTokens(), requirement.inputAndMaxOutputKvTokens(),
                requirement.priority(), requirement.capacity(), proposal.victims(), 0L, configured.getTimeoutMs(),
                () -> running.get() && !request.stage().isEnded(), "v2_decode_queue");

        DecodePreemptionCoordinator.PreemptionResult result =
                preemptionCoordinator.prepareReturnedPreemption(command).join();

        if (!result.committed()) {
            Logger.debug("V2 decode preemption not committed: request_id={} detail={}", request.requestId(),
                    result.detail());
            return null;
        }
        DecodeEndpoint.ReservationHandle reservation = endpoint.reservationHandle(request.requestId());
        if (reservation == null) {
            return null;
        }
        // RETURN 抢占沿用现有事务，先标记 queued 再取得 permit；抢占名额不做普通容量复查。
        if (!endpoint.markQueued(hold, reservation)) {
            endpoint.release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            return null;
        }

        DecodeEndpoint.EngineDispatchPermitAcquisition acquisition =
                endpoint.acquireDispatchPermit(reservation, requirement.capacity());

        if (acquisition.permit() == null) {
            endpoint.release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            return null;
        }
        serverStatus.setPreemptRequestIds(
                proposal.victims().stream().map(DecodeEndpoint.DecodeRequestView::requestId).toList());

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
        policy.getEngineCancellation().setMode(EngineCancellationConfig.Mode.RETURN);
        return policy;
    }

    private static boolean returnPreemptionAllowed(DecodeRequirement requirement, PreemptionConfig configured) {
        return requirement.mode() == DecodeMode.PREEMPT_AT_PLACEMENT
                && PriorityNormalizer.hasPriority(requirement.priority()) && configured != null
                && configured.allows(VictimStage.DECODE_ENGINE_OWNED);
    }
}
