package org.flexlb.balance.endpoint.prefill;

import org.flexlb.balance.delivery.DeliveryBlockReason;
import org.flexlb.balance.endpoint.EndpointGenerationLifecycle;
import org.flexlb.balance.endpoint.prefill.single.QueuedPreemption;
import org.flexlb.balance.endpoint.prefill.single.SinglePrefill;
import org.flexlb.balance.endpoint.prefill.single.SingleReservation;
import org.flexlb.balance.estimate.WorkView;
import org.flexlb.balance.estimate.WorkView.Phase;
import org.flexlb.balance.scheduler.ScheduledRequest;
import org.flexlb.dao.loadbalance.RejectReason;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.util.PriorityNormalizer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * 一个 Prefill worker（当前这一代）上所有请求的状态。
 *
 * <p>每个已知的 request id 只对应一个 {@link RequestEntry}。等待队列只是按顺序索引了还在等待下发的条目；
 * 直接放行的请求也占用同一份资源记录，只是不进等待队列。回调和引擎上报都直接修改这个条目，
 * 不会把请求在不同容器之间搬来搬去。</p>
 *
 * <p>Prefill 的请求表、等待队列索引和容量计数只用 worker 的 queue lock 这一把锁保护。
 * 自己加锁的公开方法会在方法里拿锁；其余方法要求调用方已经持有这把锁，方法开头用
 * {@link #requireLock()} 检查。SinglePrefill、QueuedPreemption、
 * WorkerStatusUpdater 用的也是同一把锁。</p>
 */
public final class PrefillState {

    public final ReentrantLock lock;
    /**
     * 等待队列索引，只放 ACTIVE 状态的请求，请求本身由 requests 管理。
     */
    public final PrefillActiveIndex activeIndex;
    /**
     * 所有请求的条目，只在持锁时修改。
     */
    public final Map<String, RequestEntry> requests = new HashMap<>();
    public final LongSupplier clock;
    public final Runnable capacityAvailable;
    /**
     * 只增不减的版本号，请求或工作有变化就加一，负载视图靠它判断缓存是否过期。
     */
    private volatile long mutationVersion;
    /**
     * 请求有变化时更新的在途请求数。这里只是给外面快速读的，放行时仍然要持锁重新检查。
     */
    private volatile long outstandingRequestCount;
    /**
     * 已下发的请求数，加上引擎上报但本地不认识的请求数。还在等待队列里的请求不算。
     */
    private volatile long dispatchedRequestCount;
    private long requestCountsVersion = -1;
    private long[] requestCountsByPriority;
    /**
     * 已提交工作的不可变拷贝，按需重建。等待队列的变化不影响它。
     */
    public RawWorkView cachedCommittedWork;
    public long unknownEngineRequestCount;
    public final SinglePrefill singlePrefill = new SinglePrefill(this);
    public final QueuedPreemption queuedPreemption = new QueuedPreemption(this);
    public final WorkerStatusUpdater workerStatusUpdater = new WorkerStatusUpdater(this);

    /**
     * 先更新请求计数，再加版本号，保证读到新版本号的人一定能看到新的计数。
     */
    public void recordMutation() {
        publishRequestCount();
        mutationVersion++;
    }

    public void publishRequestCount() {
        requireLock();
        long count = saturatedAdd(requests.size(), unknownEngineRequestCount);
        if (outstandingRequestCount != count) {
            outstandingRequestCount = count;
        }
        dispatchedRequestCount = saturatedAdd(
                requests.size() - activeIndex.size(), unknownEngineRequestCount);
    }

    public PrefillState(ReentrantLock lock, PrefillActiveIndex activeIndex,
                        Runnable capacityAvailable) {
        this(lock, activeIndex, System::currentTimeMillis, capacityAvailable);
    }

    public PrefillState(ReentrantLock lock,
                        PrefillActiveIndex activeIndex,
                        LongSupplier clock,
                        Runnable capacityAvailable) {
        this.lock = lock;
        this.activeIndex = activeIndex;
        this.clock = clock;
        this.capacityAvailable = capacityAvailable;
    }

    public RequestEntry putRequest(String requestId, RequestEntry entry) {
        requireLock();
        return requests.put(requestId, entry);
    }

    public boolean removeRequest(String requestId, RequestEntry entry) {
        requireLock();
        if (!requests.remove(requestId, entry)) {
            return false;
        }
        if (entry.queueMembership == QueueMembership.UNINDEXED) {
            cachedCommittedWork = null;
        }
        return true;
    }

    /**
     * 不加锁读版本号，给负载视图缓存的快速路径用，结果只作参考。
     */
    public long mutationVersion() {
        return mutationVersion;
    }

    public boolean enqueueActive(ScheduledRequest item, long maxOutstandingRequests) {
        requireLock();
        if (requests.containsKey(item.requestId())
                || (maxOutstandingRequests > 0L && !hasRequestCapacity(maxOutstandingRequests))) {
            return false;
        }
        RequestEntry entry = new RequestEntry(item);
        putRequest(item.requestId(), entry);
        try {
            activeIndex.add(item);
        } catch (RuntimeException | Error failure) {
            removeRequest(item.requestId(), entry);
            throw failure;
        }
        recordMutation();
        return true;
    }

    public boolean removeQueuedRequest(ScheduledRequest item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        if (entry == null || !entry.activeIdentity(item)) {
            return false;
        }
        Reservation lease = entry.reservation;
        requireState(lease == null || lease.state == LeaseState.OPEN,
                "ACTIVE request owns a non-OPEN Prefill lease request_id="
                        + item.requestId());
        removeFromWaitingQueue(entry, item);
        // BATCH 请求的 OPEN 批次预占和 generation handoff 一起由批次准备流程负责释放。
        // NON_BATCH 请求自己持有的单请求预占在下面和条目一起删掉，所以清理 worker 时不会看到没人管的预占。
        removeRequest(item.requestId(), entry);
        recordMutation();
        return true;
    }

    /**
     * 只有请求被拒绝时才需要算按优先级的统计，同一个版本号内复用同一份统计。
     */
    public RejectReason rejectReason(int priority, long requestLimit) {
        long[] counts;
        long occupiedRequests;
        lock.lock();
        try {
            if (requestCountsByPriority == null || requestCountsVersion != mutationVersion) {
                long[] updatedCounts = new long[PriorityNormalizer.MAX_PRIORITY + 1];
                updatedCounts[0] = unknownEngineRequestCount;
                for (RequestEntry entry : requests.values()) {
                    ScheduledRequest item = entry.activeItem != null ? entry.activeItem : entry.committedItem;
                    int occupant = item == null ? 0 : item.priority();
                    updatedCounts[occupant > 0 && occupant <= PriorityNormalizer.MAX_PRIORITY ? occupant : 0]++;
                }
                requestCountsByPriority = updatedCounts;
                requestCountsVersion = mutationVersion;
            }
            counts = requestCountsByPriority;
            occupiedRequests = outstandingRequestCount;
        } finally {
            lock.unlock();
        }
        long residual = requestLimit > 0 ? Math.max(0L, occupiedRequests + 1L - requestLimit) : 0L;
        long higher = 0L;
        long same = 0L;
        for (int occupant = 1; occupant <= PriorityNormalizer.MAX_PRIORITY; occupant++) {
            if (occupant < priority) { residual = Math.max(0L, residual - counts[occupant]); }
            else if (occupant == priority) { same += counts[occupant]; } else { higher += counts[occupant]; }
        }
        if (residual > higher + same && counts[0] > 0) {
            return RejectReason.UNSPECIFIED;
        }
        if (residual > 0 && higher + same >= residual) {
            return higher > 0 ? RejectReason.HIGHER_PRIORITY_AHEAD : RejectReason.SAME_PRIORITY_AHEAD;
        }
        return RejectReason.RESOURCE_EXHAUSTED;
    }

    public boolean canPreemptQueuedRequest(int priority, long requestLimit) {
        return queuedPreemption.canPreemptQueuedRequest(priority, requestLimit);
    }

    public List<ScheduledRequest> queuedPreemptionVictims(int priority, long requestLimit) {
        return queuedPreemption.queuedPreemptionVictims(priority, requestLimit);
    }

    public ScheduledRequest oneQueuedPreemptionVictim(int priority) {
        return queuedPreemption.oneQueuedPreemptionVictim(priority);
    }

    public SingleReservation replaceQueuedRequests(List<ScheduledRequest> victims,
                                                         List<SingleReservation> reservations,
                                                         ScheduledRequest incoming) {
        return queuedPreemption.replaceQueuedRequests(victims, reservations, incoming);
    }

    public boolean removeQueuedSingleRequest(ScheduledRequest item, SingleReservation exactReservation) {
        return singlePrefill.removeQueuedSingleRequest(item, exactReservation);
    }

    /**
     * 停止队列时，把队头请求从等待队列里摘出来交给停止回调，但先不删它的条目。
     * 这样回调失败时，清理 worker 仍然能看到这个请求；回调成功后，
     * 调用方要再调 {@link #removeStoppedRequest} 把它删掉。
     */
    public ScheduledRequest detachNextActiveForStop() {
        requireLock();
        ScheduledRequest item = activeIndex.peek();
        if (item == null) {
            return null;
        }
        RequestEntry entry = requests.get(item.requestId());
        requireState(entry != null && entry.activeIdentity(item),
                "stopped queue head has no canonical ACTIVE owner request_id="
                        + item.requestId());
        Reservation lease = entry.reservation;
        requireState(lease == null || lease.state == LeaseState.OPEN,
                "stopped ACTIVE request owns a non-OPEN Prefill lease request_id="
                        + item.requestId());
        boolean removed = activeIndex.remove(item);
        requireState(removed,
                "stopped canonical ACTIVE request has no queue index request_id="
                        + item.requestId());
        // 从队列索引里删掉之后就不能回头了。能失败的检查都已经做完，剩下只是改一个字段。
        entry.queueMembership = QueueMembership.STOP_DETACHED;
        recordMutation();
        return item;
    }

    /**
     * 停止回调完成后，删掉这个被摘出来的请求。只删同一个请求对象，其他情况不动。
     */
    public boolean removeStoppedRequest(ScheduledRequest item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        if (entry == null
                || entry.activeItem != item
                || entry.queueMembership != QueueMembership.STOP_DETACHED
                || activeIndex.contains(item)) {
            return false;
        }
        boolean removed = removeRequest(item.requestId(), entry);
        if (removed) {
            recordMutation();
        }
        return removed;
    }

    public ReservationResult<SingleReservation> reserveSingle(ScheduledRequest exactItem, long predictedMs) {
        return singlePrefill.reserveSingle(exactItem, predictedMs);
    }

    public CommitResult commitSingleGroup(List<ScheduledRequest> items,
                                          List<SingleReservation> exactReservations,
                                          EndpointGenerationLifecycle.HandoffPermit workerKeepAlive) {
        return singlePrefill.commitSingleGroup(items, exactReservations, workerKeepAlive);
    }

    public boolean canDeliver(long maxDispatchLimit, int newReqNum) {
        return dispatchedRequestCount < maxDispatchLimit
                && newReqNum <= maxDispatchLimit - dispatchedRequestCount;
    }

    public long dispatchedRequestCount() {
        return dispatchedRequestCount;
    }

    public DeliveryBlockReason.Availability routeAvailability(long maximum) {
        return new DeliveryAvailability(this, maximum);
    }

    public List<ScheduledRequest> validateActiveGroup(List<ScheduledRequest> items) {
        requireState(!items.isEmpty(), "committed group requires members");
        for (ScheduledRequest item : items) {
            RequestEntry entry = requests.get(item.requestId());
            requireState(entry != null && entry.activeIdentity(item),
                    "group member is not canonical ACTIVE request_id="
                            + item.requestId());
            requireState(activeIndex.contains(item),
                    "canonical ACTIVE request has no queue index request_id="
                            + item.requestId());
        }
        return items;
    }

    public void removeValidatedActiveIndex(ScheduledRequest item) {
        boolean removed = activeIndex.remove(item);
        requireState(removed,
                "validated ACTIVE queue index disappeared request_id="
                        + item.requestId());
    }

    public void checkQueueMembership(RequestEntry entry, ScheduledRequest item) {
        requireState(entry.queueMembership != QueueMembership.STOP_DETACHED,
                "stopped admission cannot commit request_id=" + item.requestId());
        requireState(activeIndex.contains(item) == (entry.queueMembership == QueueMembership.WAITING),
                "canonical admission and waiting index disagree request_id=" + item.requestId());
    }

    public void removeFromWaitingQueue(RequestEntry entry, ScheduledRequest item) {
        checkQueueMembership(entry, item);
        if (entry.queueMembership == QueueMembership.WAITING) {
            removeValidatedActiveIndex(item);
        }
        // 调用方在释放锁之前会删掉或提交这个条目，这里先不动条目本身。
        // 只在等待队列里的请求被删掉时，不需要让已提交工作的拷贝失效。
    }

    /**
     * 删掉一个已提交的请求，并释放它占的资源。
     * 如果 request id 已经被别的请求复用，或者请求还在等待队列里，什么都不做。
     */
    public boolean removeCommittedRequest(ScheduledRequest exactItem) {
        boolean capacityReleased = false;
        lock.lock();
        try {
            RequestEntry entry = requests.get(exactItem.requestId());
            if (entry == null || entry.isActive()
                    || entry.committedItem != exactItem) {
                return false;
            }
            // 这是本地清理，不代表引擎那边已经不做这个请求了。
            capacityReleased = removeCommittedEntry(entry);
            return true;
        } finally {
            lock.unlock();
            notifyCapacityAvailable(capacityReleased);
        }
    }

    public WorkerStatusResult updateFromWorkerStatus(WorkerStatus.StatusObservation observation,
                                                     Runnable publishStatus,
                                                     Runnable onPublishFailure) {
        return workerStatusUpdater.updateFromWorkerStatus(observation, publishStatus, onPublishFailure);
    }

    public HeartbeatResult updateFromHeartbeat(WorkerStatus.StatusObservation observation) {
        return workerStatusUpdater.updateFromHeartbeat(observation);
    }

    /**
     * Worker 下线或重启时，在一次持锁里清掉这一代的所有请求。
     * 先把回调要用的数据和结果都准备好，再清空请求表、等待队列索引和容量计数。
     * 开始清空之后只剩改字段，不会再分配内存，所以不会出现只清了一半的情况。
     *
     * <p>正常情况下 {@link org.flexlb.balance.queue.WorkerBatcher#stopAndAwait()} 已经处理完所有 ACTIVE 请求。
     * 这里还是把剩下的 ACTIVE 请求一起清掉，万一前面的检查失败了，关闭 worker 也能清干净。</p>
     */
    public ShutdownResult shutdown() {
        List<ScheduledRequest> removedItems = new ArrayList<>();
        Set<Reservation> leases = java.util.Collections.newSetFromMap(
                new IdentityHashMap<>());
        Throwable checkFailure = null;
        ShutdownResult result;
        lock.lock();
        try {
            Set<ScheduledRequest> canonicalActive = java.util.Collections.newSetFromMap(
                    new IdentityHashMap<>());
            for (Map.Entry<String, RequestEntry> canonical : requests.entrySet()) {
                RequestEntry entry = canonical.getValue();
                if (!Objects.equals(canonical.getKey(), entry.requestId)) {
                    checkFailure =
                            appendRetirementInvariant(checkFailure, "request table key does not match request entry");
                }
                ScheduledRequest item = entry.isActive() ? entry.activeItem : entry.committedItem;
                if (item == null) {
                    checkFailure = appendRetirementInvariant(checkFailure,
                            "retirement owner has no ScheduledRequest request_id=" + entry.requestId);
                } else {
                    removedItems.add(item);
                    if (entry.isActive() && entry.queueMembership == QueueMembership.WAITING) {
                        canonicalActive.add(item);
                    } else if (activeIndex.contains(item)) {
                        checkFailure = appendRetirementInvariant(checkFailure,
                                "unindexed owner remains in waiting index request_id=" + entry.requestId);
                    }
                }

                if (entry.reservation != null) {
                    leases.add(entry.reservation);
                }
            }

            if (activeIndex.size() != canonicalActive.size()) {
                checkFailure = appendRetirementInvariant(checkFailure,
                        "ACTIVE index size does not match canonical ACTIVE owners");
            }
            for (ScheduledRequest indexed : activeIndex) {
                if (!canonicalActive.contains(indexed)) {
                    checkFailure = appendRetirementInvariant(checkFailure,
                            "ACTIVE index contains a non-canonical item request_id=" + indexed.requestId());
                }
            }

            for (Reservation lease : leases) {
                if (lease.state != LeaseState.OPEN && lease.state != LeaseState.OWNED) {
                    checkFailure = appendRetirementInvariant(checkFailure,
                            "canonical retirement owner has an invalid reservation state");
                }
            }
            removedItems.sort(
                    Comparator.comparingLong(ScheduledRequest::enqueueSeq).thenComparing(ScheduledRequest::requestId));
            result = new ShutdownResult(removedItems, checkFailure);

            // 从这里开始真正清空。会分配内存或做检查的步骤都已经在上面做完了。
            for (RequestEntry entry : requests.values()) {
                entry.activeItem = null;
                entry.committedItem = null;
                entry.reservation = null;
                entry.singleRequestProgress = null;
                entry.queueMembership = QueueMembership.UNINDEXED;
            }
            for (Reservation lease : leases) {
                lease.state = LeaseState.CLOSED;
            }
            requests.clear();
            activeIndex.clear();
            unknownEngineRequestCount = 0L;
            cachedCommittedWork = null;
            recordMutation();
        } finally {
            lock.unlock();
        }

        notifyCapacityAvailable(!leases.isEmpty());
        return result;
    }

    public static Throwable appendRetirementInvariant(Throwable first, String message) {
        IllegalStateException next = new IllegalStateException(message);
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    public int evictExpiredSingles(long ttlMs, java.util.function.Predicate<String> retainForSchedulerCleanup) {
        return singlePrefill.evictExpiredSingles(ttlMs, retainForSchedulerCleanup);
    }

    public PrefillStats stats() {
        lock.lock();
        try {
            int committed = 0;
            int waitingForWorkerStatus = 0;
            long maxAgeMs = 0L;
            long nowMs = clock.getAsLong();
            for (RequestEntry entry : requests.values()) {
                if (entry.isActive()) {
                    continue;
                }
                committed++;
                if (entry.singleRequestProgress.phase() == Phase.COMMITTED
                        && entry.reservation != null && entry.reservation.state == LeaseState.OWNED) {
                    waitingForWorkerStatus++;
                }
                maxAgeMs = Math.max(maxAgeMs, Math.max(0L, nowMs - entry.singleRequestProgress.lastObservedAtMs()));
            }
            return new PrefillStats(saturatedAdd(requests.size(), unknownEngineRequestCount), committed,
                    waitingForWorkerStatus, committed, maxAgeMs);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 选 worker 时的粗略检查，返回 true 不代表已经占到了容量。
     */
    public boolean canAcceptRequest(long requestLimit) {
        return outstandingRequestCount < requestLimit;
    }

    private boolean hasRequestCapacity(long maxOutstandingRequests) {
        requireLock();
        return requests.size() < maxOutstandingRequests
                && unknownEngineRequestCount < maxOutstandingRequests - requests.size();
    }

    public long observedRequestCount() {
        lock.lock();
        try {
            return saturatedAdd(
                    requests.size(),
                    unknownEngineRequestCount);
        } finally {
            lock.unlock();
        }
    }

    public PrefillLoadView loadView() {
        requireLock();
        long nowMs = clock.getAsLong();
        return new PrefillLoadView(nowMs, activeIndex.waitingQueueView(), cachedCommittedWork(nowMs));
    }

    public WorkView committedSnapshot() {
        RawWorkView rawView;
        lock.lock();
        try {
            rawView = copyCommittedWork(clock.getAsLong());
        } finally {
            lock.unlock();
        }
        return rawView.toWorkView();
    }

    private RawWorkView cachedCommittedWork(long nowMs) {
        requireLock();
        if (cachedCommittedWork == null || cachedCommittedWork.createdAtMs > nowMs) {
            cachedCommittedWork = copyCommittedWork(nowMs);
        }
        return cachedCommittedWork;
    }

    public WorkView buildPrecedingWork(List<ScheduledRequest> members, long nowMs) {
        requireLock();
        Set<RequestEntry> excluded = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (ScheduledRequest member : members) {
            excluded.add(requests.get(member.requestId()));
        }
        return copyCommittedWork(nowMs, excluded).toWorkView();
    }

    private RawWorkView copyCommittedWork(long nowMs) {
        return copyCommittedWork(nowMs, Set.of());
    }

    private RawWorkView copyCommittedWork(long nowMs, Set<RequestEntry> excluded) {
        requireLock();
        List<WorkView.RequestWork> single = new ArrayList<>();
        for (RequestEntry entry : requests.values()) {
            if (excluded.contains(entry)) {
                continue;
            }
            if (entry.isActive()) {
                continue;
            }
            single.add(new WorkView.RequestWork(entry.requestId, entry.singleRequestProgress.phase(),
                    entry.singleRequestProgress.remainingWorkMsAt(nowMs)));
        }
        return new RawWorkView(nowMs, single, unknownEngineRequestCount);
    }

    public boolean removeCommittedEntry(RequestEntry entry) {
        requireLock();
        Reservation lease = entry.reservation;
        if (lease != null && lease.state != LeaseState.OWNED) {
            throw new IllegalStateException(
                    "terminal request owns a non-committed Prefill lease"
                            + " request_id=" + entry.requestId);
        }
        if (!removeRequest(entry.requestId, entry)) {
            throw new IllegalStateException(
                    "terminal request is not canonical request_id="
                            + entry.requestId);
        }
        cachedCommittedWork = null;
        recordMutation();
        if (lease != null) {
            lease.state = LeaseState.CLOSED;
        }
        return true;
    }

    /**
     * 只回滚还没提交的预占。一旦提交，资源就归请求表管，只能在请求结束时释放。
     */
    void releaseOpenLease(Reservation lease) {
        if (lock.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "Prefill lease rollback cannot run under queueLock");
        }
        boolean capacityReleased = false;
        lock.lock();
        try {
            if (lease.state == LeaseState.OPEN) {
                closeOpenLease(lease);
                capacityReleased = true;
            }
        } finally {
            lock.unlock();
        }
        notifyCapacityAvailable(capacityReleased);
    }

    public void notifyCapacityAvailable(boolean capacityReleased) {
        if (!capacityReleased) {
            return;
        }
        try {
            capacityAvailable.run();
        } catch (Throwable notificationFailure) {
            try {
                org.flexlb.util.Logger.error(
                        "Prefill capacity notification failed",
                        notificationFailure);
            } catch (Throwable ignoredLoggingFailure) {
                // 容量已经释放完了，打日志失败也不能影响调用方已经准备好的结束结果。
            }
        }
    }

    /**
     * 回滚 OPEN 状态的请求预占。
     */
    public void closeOpenLease(Reservation lease) {
        requireLock();
        if (lease.state != LeaseState.OPEN) {
            throw new IllegalStateException(
                    "Prefill OPEN rollback lost its exact lease");
        }
        RequestEntry owner = openLeaseOwner(lease);
        if (owner != null
                && !owner.isActive()
                && owner.queueMembership != QueueMembership.STOP_DETACHED) {
            throw new IllegalStateException(
                    "OPEN Prefill lease has a non-ACTIVE canonical owner");
        }
        lease.state = LeaseState.CLOSED;
        if (owner != null) {
            owner.reservation = null;
        }
        recordMutation();
    }

    private RequestEntry openLeaseOwner(Reservation lease) {
        requireLock();
        RequestEntry originalOwner = lease.originalOwner;
        RequestEntry current = requests.get(originalOwner.requestId);
        if (current == originalOwner) {
            if (current.reservation != lease) {
                throw new IllegalStateException(
                        "canonical Prefill lease owner lost its exact reservation"
                                + " request_id=" + originalOwner.requestId);
            }
            return current;
        }
        if (current != null && current.reservation == lease) {
            throw new IllegalStateException(
                    "replacement request cannot own an earlier Prefill lease"
                            + " request_id=" + originalOwner.requestId);
        }
        return null;
    }

    public void requireLock() {
        requireState(lock.isHeldByCurrentThread(),
                "Prefill ownership requires queueLock");
    }

    public static void requireState(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    public static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
