package org.flexlb.cache;

import org.flexlb.dao.master.CacheStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.KvCacheGroupMode;

import java.util.Map;

public final class WorkerStatusTestSupport {

    private WorkerStatusTestSupport() {
    }

    public static WorkerStatus workerStatus(String ip, int port, RoleType role) {
        return workerStatus(ip, port, role, false, null, 0);
    }

    public static WorkerStatus workerStatus(
            String ip, int port, RoleType role, boolean alive, CacheStatus cacheStatus) {
        return workerStatus(ip, port, role, alive, cacheStatus, 0);
    }

    public static WorkerStatus workerStatus(String ip,
                                            int port,
                                            RoleType role,
                                            boolean alive,
                                            CacheStatus cacheStatus,
                                            int cacheMatchRollbackBlocks) {
        WorkerStatus workerStatus =
                WorkerStatus.createDiscovered(role, "default", ip, port, port + 1, "", null, 0, 1, false);
        publishObservation(workerStatus, role, alive, cacheMatchRollbackBlocks);
        if (cacheStatus != null) {
            workerStatus.lock.lock();
            try {
                workerStatus.publishCacheStatus(cacheStatus);
            } finally {
                workerStatus.lock.unlock();
            }
        }
        return workerStatus;
    }

    private static void publishObservation(WorkerStatus workerStatus,
                                           RoleType role,
                                           boolean alive,
                                           int cacheMatchRollbackBlocks) {
        WorkerStatus.EngineObservation engine = new WorkerStatus.EngineObservation(role, 0L, 0L, 0L, Map.of(), 0.0, 0L,
                0L, 0L, 0L, null, 0L, 0, cacheMatchRollbackBlocks, KvCacheGroupMode.UNSPECIFIED, 0L, 0L, 0L, 0L, null);

        workerStatus.lock.lock();
        try {
            WorkerStatus.StatusObservation observation =
                    workerStatus.bindStatusObservation(engine, alive, 1L, 0L, Map.of());
            WorkerStatus.PreparedStatus prepared =
                    workerStatus.prepareNewStatus(observation);
            workerStatus.publishPreparedStatus(prepared);
            workerStatus.recordSuccessfulPoll(alive);
        } finally {
            workerStatus.lock.unlock();
        }
    }
}
