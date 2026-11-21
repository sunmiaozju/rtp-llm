package org.flexlb.balance.schedulev2;

import org.flexlb.balance.schedulev2.request.Request;
import org.flexlb.balance.schedulev2.request.Stage;
import org.flexlb.balance.strategy.PrefillInputs;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public final class RequestPreparePool implements AutoCloseable {

    private final ExecutorService executor;
    private final LoopSignal signal;

    public RequestPreparePool(int coreThreads, LoopSignal signal) {
        this(createExecutor(coreThreads), signal);
    }

    public RequestPreparePool(ExecutorService executor, LoopSignal signal) {
        this.executor = executor;
        this.signal = signal;
    }

    private static ExecutorService createExecutor(int coreThreads) {
        int maxThreads = Math.multiplyExact(coreThreads, 3);
        AtomicInteger workerId = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, "flexlb-v2-main-worker-" + workerId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return new ThreadPoolExecutor(coreThreads, maxThreads, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(coreThreads), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    public void submit(Request request, Supplier<PrefillInputs> prepare) {
        if (request.stage() != Stage.MAIN_WAITING || request.getWorkLoadEstimate() != null) {
            return;
        }

        CompletableFuture<SelectionResult<PrefillInputs>> estimateFuture = new CompletableFuture<>();
        request.setWorkLoadEstimate(estimateFuture);
        try {
            executor.execute(() -> {
                try {
                    if (request.stage() != Stage.MAIN_WAITING) {
                        return;
                    }
                    estimateFuture.complete(new SelectionResult<>(prepare.get(), null, System.nanoTime()));
                } catch (Throwable failure) {
                    estimateFuture.complete(new SelectionResult<>(null, failure, System.nanoTime()));
                } finally {
                    signal.wake();
                }
            });
        } catch (RejectedExecutionException rejected) {
            request.setWorkLoadEstimate(null);
            request.ctx().getMetrics().recordPrepareSubmitRejected();
        } catch (RuntimeException | Error failure) {
            request.setWorkLoadEstimate(null);
            throw failure;
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
