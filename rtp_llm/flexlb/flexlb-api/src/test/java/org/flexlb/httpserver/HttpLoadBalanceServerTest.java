package org.flexlb.httpserver;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.RequestScheduler;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.consistency.LBStatusConsistencyService;
import org.flexlb.dao.master.CacheStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.domain.consistency.MasterChangeNotifyResp;
import org.flexlb.sync.status.WorkerDirectory;
import org.flexlb.sync.synchronizer.MasterEngineSynchronizer;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HttpLoadBalanceServerTest {

    @Test
    void inflightStatusExposesExactBlockedRequestCount() {
        RequestScheduler scheduler = mock(RequestScheduler.class);
        when(scheduler.getTrackedRequestCount()).thenReturn(9);
        when(scheduler.getBlockedRequestCount()).thenReturn(2);
        HttpLoadBalanceServer server = new HttpLoadBalanceServer(
                mock(LBStatusConsistencyService.class),
                mock(ConfigService.class, RETURNS_DEEP_STUBS),
                scheduler,
                mock(WorkerDirectory.class),
                mock(MasterEngineSynchronizer.class),
                new ServerScheduleLatencyRecorder());
        WebTestClient client = WebTestClient
                .bindToRouterFunction(server.loadBalancePrefill())
                .build();

        client.get()
                .uri("/rtp_llm/inflight_status")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.scheduler_inflight").isEqualTo(9)
                .jsonPath("$.scheduler_tracked").isEqualTo(9)
                .jsonPath("$.scheduler_blocked").isEqualTo(2);

        verify(scheduler).getBlockedRequestCount();
    }

    @Test
    void masterInfoUsesCanonicalSchedulerQueueDepth() {
        LBStatusConsistencyService consistency = mock(LBStatusConsistencyService.class);
        ConfigService configService = mock(ConfigService.class);
        RequestScheduler scheduler = mock(RequestScheduler.class);
        MasterEngineSynchronizer synchronizer = mock(MasterEngineSynchronizer.class);
        when(consistency.getMasterHostIpPort()).thenReturn("127.0.0.1:7001");
        when(scheduler.getQueuedRequestCount()).thenReturn(7);
        when(synchronizer.isReady()).thenReturn(true);

        HttpLoadBalanceServer server = new HttpLoadBalanceServer(
                consistency,
                configService,
                scheduler,
                mock(WorkerDirectory.class),
                synchronizer,
                new ServerScheduleLatencyRecorder());
        WebTestClient client = WebTestClient
                .bindToRouterFunction(server.loadBalancePrefill())
                .build();

        client.post()
                .uri("/rtp_llm/master/info")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.queue_length").isEqualTo(7)
                .jsonPath("$.real_master_host").isEqualTo("127.0.0.1:7001")
                .jsonPath("$.ready").isEqualTo(true);

        verify(scheduler).getQueuedRequestCount();
    }

    @Test
    void inflightStatusExposesObservedAndIndexedCacheVersions() {
        String address = "127.0.0.1:8080";
        String endpointPath = "$.prefill_endpoints[?(@.ip_port == '%s')]".formatted(address);
        ConfigService configService = mock(ConfigService.class);
        RequestScheduler scheduler = mock(RequestScheduler.class);
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        var queue = mock(org.flexlb.balance.schedulev2.prefill.PrefillQueue.class);
        when(endpoint.prefillQueue()).thenReturn(queue);
        when(queue.dispatchedCount()).thenReturn(3);
        WorkerStatus status = mock(WorkerStatus.class);
        when(configService.loadBalanceConfig()).thenReturn(new FlexlbConfig());
        when(endpoint.getStatus()).thenReturn(status);
        when(endpoint.ipPort()).thenReturn(address);
        when(status.cacheIndexSnapshot()).thenReturn(
                new WorkerStatus.CacheIndexSnapshot(
                        CacheStatus.builder()
                                .version(9L)
                                .cacheKeySize(4L)
                                .build(),
                        true,
                        8L));
        var endpoints = EndpointRegistry.getEndpointsByRole(RoleType.PREFILL);
        endpoints.put(address, endpoint);
        try {
            HttpLoadBalanceServer server = new HttpLoadBalanceServer(mock(LBStatusConsistencyService.class),
                    configService, scheduler, mock(WorkerDirectory.class), mock(MasterEngineSynchronizer.class),
                    new ServerScheduleLatencyRecorder());
            WebTestClient client = WebTestClient.bindToRouterFunction(server.loadBalancePrefill()).build();

            client.get()
                    .uri("/rtp_llm/inflight_status")
                    .exchange()
                    .expectStatus()
                    .isOk()
                    .expectBody()
                    .jsonPath(endpointPath + ".inflight_requests").isEqualTo(3)
                    .jsonPath(endpointPath + ".inflight_route_requests").isEqualTo(3)
                    .jsonPath(endpointPath + ".cache_version").isEqualTo(9)
                    .jsonPath(endpointPath + ".cache_indexed").isEqualTo(true)
                    .jsonPath(endpointPath + ".cache_indexed_version").isEqualTo(8)
                    .jsonPath(endpointPath + ".cache_key_size").isEqualTo(4);
        } finally {
            endpoints.remove(address, endpoint);
        }
    }

    @Test
    void notifyMasterSerializesTheResponseContract() {
        LBStatusConsistencyService consistency =
                mock(LBStatusConsistencyService.class);
        MasterChangeNotifyResp response = new MasterChangeNotifyResp();
        response.setSuccess(true);
        response.setMsg("refreshed");
        when(consistency.handleMasterChange(org.mockito.ArgumentMatchers.any()))
                .thenReturn(response);
        HttpLoadBalanceServer server = new HttpLoadBalanceServer(
                consistency,
                mock(ConfigService.class),
                mock(RequestScheduler.class),
                mock(WorkerDirectory.class),
                mock(MasterEngineSynchronizer.class),
                new ServerScheduleLatencyRecorder());
        WebTestClient client = WebTestClient
                .bindToRouterFunction(server.loadBalancePrefill())
                .build();

        client.post()
                .uri("/rtp_llm/notify_master")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"roleId\":\"role-a\"}")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.success").isEqualTo(true)
                .jsonPath("$.msg").isEqualTo("refreshed");
    }
}
