package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 启动时创建固定数量的 Decode 消费线程，关闭时唤醒并等待消费者归还未发布资源。
 * 不使用额外任务队列，不在运行中扩缩线程数。
 */
public final class DecodeQueueConsumerPoll {

    private final List<DecodeQueueConsumer> consumers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final PlacementAvailability placementAvailability;
    private final PlacementAvailability.Listener availabilityListener;
    private boolean started;
    private boolean closed;

    public DecodeQueueConsumerPoll(int count,
                                   DecodeQueue queue,
                                   PlacementAvailability placementAvailability,
                                   Supplier<DecodeQueueConsumer> consumerFactory) {
        this.placementAvailability = placementAvailability;
        this.availabilityListener = event -> {
            RoleType role = event.key().role();
            if (role == RoleType.DECODE || role == RoleType.PREFILL || role == RoleType.PDFUSION) {
                queue.wake();
            }
        };
        for (int i = 0; i < count; i++) {
            DecodeQueueConsumer consumer = consumerFactory.get();
            consumers.add(consumer);
            Thread thread = new Thread(consumer, "flexlb-v2-decode-consumer-" + (i + 1));
            thread.setDaemon(true);
            threads.add(thread);
        }
    }

    public synchronized void start() {
        if (started || closed) {
            return;
        }
        started = true;
        // 1. 先监听全体 Decode 的容量及上下线变化，再启动消费者，不限于上一次选中的节点。
        placementAvailability.addListener(availabilityListener);
        threads.forEach(Thread::start);
    }

    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            placementAvailability.removeListener(availabilityListener);
            consumers.forEach(DecodeQueueConsumer::close);
        }
        // 2. 不中断资源操作；等待线程自行退出并归还，每个池合计最多等待 10 秒。
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        for (Thread thread : threads) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                Logger.warn("V2 decode consumers did not stop within shutdown timeout");
                return;
            }
            try {
                TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
                if (thread.isAlive()) {
                    Logger.warn("V2 decode consumers did not stop within shutdown timeout");
                    return;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
