package org.flexlb.balance.schedulev2;

import org.flexlb.balance.schedulev2.main.MainQueue;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.RequestWorkLoadEstimateResult;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.dao.route.RoleType;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class RequestPreparePool implements AutoCloseable {

    private final ExecutorService executor;
    private final MainQueue mainQueue;
    private final PrefillPicker prefillPicker;
    private final RoleType prefillRole;

    public RequestPreparePool(DispatcherConfig dispatcher,
                              MainQueue mainQueue,
                              PrefillPicker prefillPicker,
                              RoleType prefillRole) {
        this(createExecutor(dispatcher), mainQueue, prefillPicker, prefillRole);
    }

    public RequestPreparePool(ExecutorService executor,
                              MainQueue mainQueue,
                              PrefillPicker prefillPicker,
                              RoleType prefillRole) {
        this.executor = executor;
        this.mainQueue = mainQueue;
        this.prefillPicker = prefillPicker;
        this.prefillRole = prefillRole;
    }

    private static ExecutorService createExecutor(DispatcherConfig dispatcher) {
        int coreThreads = dispatcher.getRequestPrepareThreads();
        int maxThreads = dispatcher.getRequestPrepareMaxThreads();
        AtomicInteger workerId = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, "flexlb-v2-main-worker-" + workerId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return new ThreadPoolExecutor(coreThreads, maxThreads, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(coreThreads), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    public void submit(Request request, String group) {
        executor.execute(() -> prepareAndAddBack(request, group));
    }

    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public void prepareAndAddBack(Request request, String group) {
        if (request.stage() != Stage.MAIN_WAITING) {
            return;
        }

        RequestWorkLoadEstimateResult estimate;
        try {
            estimate = prefillPicker.prepare(request.ctx(), prefillRole, group);
        } catch (Throwable failure) {
            estimate = RequestWorkLoadEstimateResult.failed(failure);
        }

        if (request.stage() == Stage.MAIN_WAITING) {
            request.setWorkLoadEstimate(estimate);
            mainQueue.addBackAndWake(request);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
