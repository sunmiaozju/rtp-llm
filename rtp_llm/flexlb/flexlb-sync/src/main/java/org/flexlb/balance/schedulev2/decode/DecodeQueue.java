package org.flexlb.balance.schedulev2.decode;

import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.RequestOrder;
import org.flexlb.balance.schedulev2.request.Stage;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BooleanSupplier;

/**
 * 全局 Decode 等待队列，按 {@link RequestOrder} 排序。
 * 入队请求已占 Prefill 名额，消费者直接取队首选 Decode。空队列时阻塞等待通知。
 */
public final class DecodeQueue {

    private final ConcurrentSkipListSet<Request> waiting;
    private volatile long changeVersion;

    public DecodeQueue(boolean priorityOrdering) {
        this.waiting = new ConcurrentSkipListSet<>(RequestOrder.of(priorityOrdering));
    }

    public void addWaiting(Request request) {
        if (request.stage() != Stage.DECODE_WAITING) {
            return;
        }
        waiting.add(request);
    }

    public void removeWaiting(Request request) {
        waiting.remove(request);
    }

    public int waitingCount() {
        return waiting.size();
    }

    public Request take(BooleanSupplier running) throws InterruptedException {
        while (running.getAsBoolean()) {
            long version = changeVersion;
            Request request = waiting.pollFirst();
            if (request == null) {
                if (running.getAsBoolean()) {
                    //noinspection BusyWait
                    Thread.sleep(10);
                }
            } else if (request.stage() == Stage.DECODE_WAITING) {
                return request;
            }
        }
        return null;
    }

    public long changeVersion() {
        return changeVersion;
    }

    public synchronized void wake() {
        changeVersion++;
        notifyAll();
    }

}
