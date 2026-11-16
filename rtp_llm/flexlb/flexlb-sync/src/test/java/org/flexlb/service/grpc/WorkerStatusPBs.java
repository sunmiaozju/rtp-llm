package org.flexlb.service.grpc;

import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.PriorityPreemptionProgressPB;
import org.flexlb.engine.grpc.EngineRpcService.RoleTypePB;
import org.flexlb.engine.grpc.EngineRpcService.TaskInfoPB;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.enums.PriorityPreemptionProgress;
import org.flexlb.enums.TaskPhase;

import java.util.Map;

/**
 * 构造引擎状态的 protobuf 测试数据；状态转换由生产的 {@link EngineStatusConverter} 完成。
 */
public final class WorkerStatusPBs {

    private WorkerStatusPBs() {
    }

    public static WorkerStatusPB.Builder status(RoleType role) {
        return WorkerStatusPB.newBuilder().setRoleType(role(role));
    }

    /**
     * 替换当前任务列表；Map 只提供任务值，转换器使用消息中的 requestId 作为任务键。
     */
    public static WorkerStatusPB.Builder setRunningTasks(WorkerStatusPB.Builder status,
                                                         Map<String, TaskInfoPB.Builder> tasks) {
        status.clearRunningTaskInfo();
        if (tasks != null) {
            tasks.values().forEach(status::addRunningTaskInfo);
        }
        return status;
    }

    /**
     * 替换完成任务列表；Map 只提供任务值，转换器使用消息中的 requestId 作为任务键。
     */
    public static WorkerStatusPB.Builder setFinishedTasks(WorkerStatusPB.Builder status,
                                                          Map<String, TaskInfoPB.Builder> tasks) {
        status.clearFinishedTaskList();
        if (tasks != null) {
            tasks.values().forEach(status::addFinishedTaskList);
        }
        return status;
    }

    public static TaskInfoPB.Builder task(String requestId) {
        return TaskInfoPB.newBuilder().setRequestIdStr(requestId);
    }

    public static TaskInfoPB.Builder task(String requestId, TaskPhase phase) {
        return phase(task(requestId), phase);
    }

    /**
     * PENDING 同时写入 is_waiting，否则协议默认值会被解释成旧版 RUNNING。
     */
    public static TaskInfoPB.Builder phase(TaskInfoPB.Builder task, TaskPhase phase) {
        if (phase == null) {
            return task.clearPhase().clearIsWaiting();
        }
        task.clearIsWaiting();
        return switch (phase) {
            case PENDING ->
                task.setPhase(org.flexlb.engine.grpc.EngineRpcService.TaskPhase.TASK_PHASE_PENDING).setIsWaiting(true);
            case RECEIVED -> task.setPhase(org.flexlb.engine.grpc.EngineRpcService.TaskPhase.TASK_PHASE_RECEIVED);
            case KV_ALLOCATED ->
                task.setPhase(org.flexlb.engine.grpc.EngineRpcService.TaskPhase.TASK_PHASE_KV_ALLOCATED);
            case RUNNING -> task.setPhase(org.flexlb.engine.grpc.EngineRpcService.TaskPhase.TASK_PHASE_RUNNING);
        };
    }

    public static PriorityPreemptionProgressPB progress(PriorityPreemptionProgress progress) {
        return switch (progress) {
            case NONE -> PriorityPreemptionProgressPB.PRIORITY_PREEMPTION_NONE;
            case CANCELING -> PriorityPreemptionProgressPB.PRIORITY_PREEMPTION_CANCELING;
            case CANCELED -> PriorityPreemptionProgressPB.PRIORITY_PREEMPTION_CANCELED;
        };
    }

    public static RoleTypePB role(RoleType role) {
        return switch (role) {
            case PDFUSION -> RoleTypePB.ROLE_TYPE_PDFUSION;
            case PREFILL -> RoleTypePB.ROLE_TYPE_PREFILL;
            case DECODE -> RoleTypePB.ROLE_TYPE_DECODE;
            case VIT -> RoleTypePB.ROLE_TYPE_VIT;
            case FRONTEND -> RoleTypePB.ROLE_TYPE_FRONTEND;
            case ENCODER -> RoleTypePB.ROLE_TYPE_ENCODER;
        };
    }
}
