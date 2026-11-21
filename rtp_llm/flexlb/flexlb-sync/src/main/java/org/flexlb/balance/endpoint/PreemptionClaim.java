package org.flexlb.balance.endpoint;

import lombok.Getter;
import lombok.Setter;

/**
 * 保留被选中抢占的请求容量，等待 Worker 状态确认或归还未发布的路由。
 */
@Getter
@Setter
public final class PreemptionClaim {
    final long attemptToken;
    ClaimOwner owner;
    final long requiredKvTokens;
    final long kvBudgetTokens;
    boolean kvHeldAfterWorkerRelease;

    public PreemptionClaim(long attemptToken, ClaimOwner owner, long requiredKvTokens, long kvBudgetTokens) {
        if (requiredKvTokens < 0L || kvBudgetTokens < requiredKvTokens) {
            throw new IllegalArgumentException("Priority preemption claim requires expected KV >= hard KV >= 0");
        }
        this.attemptToken = attemptToken;
        this.owner = owner;
        this.requiredKvTokens = requiredKvTokens;
        this.kvBudgetTokens = kvBudgetTokens;
    }
}
