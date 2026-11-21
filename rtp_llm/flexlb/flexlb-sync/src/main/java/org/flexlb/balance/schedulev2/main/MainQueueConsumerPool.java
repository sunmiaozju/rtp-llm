package org.flexlb.balance.schedulev2.main;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.scheduler.QueueCapacity;
import org.flexlb.balance.schedulev2.LoopSignal;
import org.flexlb.balance.schedulev2.RequestCleanup;
import org.flexlb.balance.schedulev2.RequestPreparePool;
import org.flexlb.balance.schedulev2.prefill.PrefillPicker;
import org.flexlb.balance.schedulev2.prefill.PrefillQueueRegistry;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.V2QueueReporter;

import java.util.ArrayList;
import java.util.List;

public final class MainQueueConsumerPool {

    private final List<Thread> threads = new ArrayList<>();
    private final LoopSignal signal;
    private final RequestPreparePool preparePool;
    private final PlacementAvailability placementAvailability;
    private final PlacementAvailability.Listener prefillChangeListener;
    private volatile boolean running;
    private boolean closed;

    public MainQueueConsumerPool(MainQueue mainQueue,
                                 LoopSignal signal,
                                 RequestCleanup cleanup,
                                 QueueCapacity queueCapacity,
                                 EndpointRegistry endpointRegistry,
                                 PlacementAvailability placementAvailability,
                                 ConfigService configService,
                                 DefaultRouter router,
                                 CostBasedPrefillStrategy prefillStrategy,
                                 PrefillQueueRegistry prefillQueues,
                                 RoleType prefillRole,
                                 V2QueueReporter queueReporter) {
        this.signal = signal;
        this.placementAvailability = placementAvailability;
        this.prefillChangeListener = event -> {
            if (event.key().role() == prefillRole) {
                signal.wake();
            }
        };

        DispatcherConfig dispatcher = configService.loadBalanceConfig().getDispatcher();
        this.preparePool = new RequestPreparePool(dispatcher.getRequestPrepareThreads(), signal);
        PrefillPicker picker = new PrefillPicker(router, prefillStrategy);
        int consumerThreads = dispatcher.getMainQueueConsumerThreads();
        for (int i = 0; i < consumerThreads; i++) {
            MainQueueConsumer consumer = new MainQueueConsumer(mainQueue, signal, cleanup, queueCapacity,
                    endpointRegistry, configService, picker, prefillQueues, prefillRole, prefillQueues::addWaiting,
                    preparePool, queueReporter);
            Thread thread = new Thread(() -> consumer.run(() -> running), "flexlb-v2-main-consumer-" + (i + 1));
            thread.setDaemon(true);
            threads.add(thread);
        }
    }

    public synchronized void start() {
        if (running || closed) {
            return;
        }
        placementAvailability.addListener(prefillChangeListener);
        running = true;
        threads.forEach(Thread::start);
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (running) {
            running = false;
            placementAvailability.removeListener(prefillChangeListener);
            signal.wake();
        }
        preparePool.close();
    }
}
