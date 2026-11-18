package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestOrder;
import org.flexlb.balance.schedulev2.request.Stage;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * 全局 Decode 等待队列，按 {@link RequestOrder} 排序。
 * 原子 remove 成功的消费者负责检查 Prefill 名额；名额不足就按原顺序放回，并继续尝试后面的请求。
 * 已取得名额的请求不在集合中，由该消费者负责选 Decode、预占、发布或归还。
 * 无可消费请求时阻塞等待通知；队列扫描和容量检查不持有通知锁。
 */
public final class DecodeQueue {

    private final ConcurrentSkipListSet<Request> waiting;
    private volatile long changeVersion;

    public DecodeQueue(boolean priorityOrdering) {
        this.waiting = new ConcurrentSkipListSet<>(RequestOrder.of(priorityOrdering));
    }

    public void addWaiting(Request request) {
        addBack(request);
        wake();
    }

    public void removeWaiting(Request request) {
        waiting.remove(request);
    }

    public int waitingCount() {
        return waiting.size();
    }

    public Request take(Predicate<Request> tryAcquirePrefill, BooleanSupplier running) throws InterruptedException {
        while (running.getAsBoolean()) {
            // 1. 扫描前记下通知版本，扫描期间资源变化后不应再睡眠。
            long version = changeVersion;
            Iterator<Request> iterator = waiting.iterator();
            while (running.getAsBoolean() && iterator.hasNext()) {
                Request request = iterator.next();
                // 2. remove 成功才认领；其他消费者已经取走的请求不能再获取 Prefill 名额。
                if (!waiting.remove(request)) {
                    continue;
                }
                // 3. Prefill 名额已取得才交给消费者等待 Decode，名额不足的请求不阻塞后续请求。
                if (tryAcquirePrefill.test(request)) {
                    return request;
                }
                // 4. 排序键不变，放回原位置；本轮迭代器继续向后，不重新读取这个队头。
                addBack(request);
            }
            if (running.getAsBoolean()) {
                // 5. 一整轮没有可消费请求才等待，不持有集合锁或临时 Prefill 名额。
                awaitChange(version);
            }
        }
        return null;
    }

    private void addBack(Request request) {
        if (request.stage() != Stage.DECODE_WAITING) {
            return;
        }
        waiting.add(request);
    }

    public long changeVersion() {
        return changeVersion;
    }

    public synchronized void wake() {
        changeVersion++;
        notifyAll();
    }

    /**
     * 版本检查避免检查容量与开始等待之间丢通知；50ms 兜底未发送通知的变化。
     */
    public synchronized void awaitChange(long version) throws InterruptedException {
        if (version == changeVersion) {
            wait(50L);
        }
    }

}
