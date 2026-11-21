package org.flexlb.balance.delivery;

import org.flexlb.balance.estimate.EndpointPrefillLoadProvider;
import org.flexlb.balance.estimate.NonBatchTtftEstimator;
import org.flexlb.balance.estimate.PrefillLoadProvider;
import org.flexlb.balance.estimate.PrefillTtftEstimator;
import org.flexlb.balance.scheduler.RequestRegistry;
import org.flexlb.balance.schedulev2.prefill.QueuePrefillLoadProvider;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class DeliveryConfiguration {

    @Bean
    public DeliveryStrategy deliveryStrategy(RequestRegistry requests, BatchSchedulerReporter reporter) {
        return new NonBatchDeliveryStrategy(requests, new DeliveryMetrics(reporter));
    }

    @Bean
    public PrefillTtftEstimator prefillTtftEstimator() {
        return new NonBatchTtftEstimator();
    }

    @Bean
    public PrefillLoadProvider prefillLoadProvider(ConfigService configService) {
        return switch (dispatcherType(configService)) {
            case NON_BATCH -> new EndpointPrefillLoadProvider();
            case NON_BATCH_V2 -> new QueuePrefillLoadProvider(configService);
        };
    }

    private static DispatcherConfig.Type dispatcherType(ConfigService configService) {
        return configService.loadBalanceConfig().getDispatcher().getType();
    }
}
