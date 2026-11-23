package org.flexlb.sync.status;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.SchedulingTestConfig;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.cache.match.CacheAwareService;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusProvider;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.WorkerStatusPB;
import org.flexlb.service.grpc.EngineStatusConverter;
import org.flexlb.sync.runner.RunnerTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerDirectoryTest {

    private WorkerDirectory workerDirectory;
    private EndpointRegistry registry;

    private record DecodeMember(WorkerStatus status, WorkerStatusPB.Builder response) {
    }

    private record PrefillMember(WorkerStatus status, WorkerStatusPB.Builder response) {
    }

    @BeforeEach
    void setUp() {
        for (RoleType role : RoleType.values()) {
            var endpoints = EndpointRegistry.getEndpointsByRole(role);
            if (endpoints != null) {
                endpoints.clear();
            }
        }
        ConfigService configService = Mockito.mock(ConfigService.class);
        Mockito.when(configService.loadBalanceConfig()).thenReturn(SchedulingTestConfig.newConfig());
        registry = RunnerTestSupport.endpointRegistry(configService);
        workerDirectory = new WorkerDirectory(registry);
    }

    @Test
    void routing_capacity_comes_only_from_published_endpoints() {
        WorkerStatus matching = status(RoleType.DECODE, "group1", 8080);
        WorkerStatus filtered = status(RoleType.DECODE, "group2", 8081);
        discover(matching);
        discover(filtered);
        RunnerTestSupport.publishEndpoint(registry,
                RoleType.DECODE, matching.getLogicalIpPort(), matching);
        RunnerTestSupport.publishEndpoint(registry,
                RoleType.DECODE, filtered.getLogicalIpPort(), filtered);

        assertEquals(2,
                workerDirectory.routingCapacity(RoleType.DECODE));
    }

    @Test
    void should_hold_pd_fusion_and_vit_from_role_specific_registries() {
        assertRoleEndpoint(RoleType.PDFUSION, 8101);
        assertRoleEndpoint(RoleType.VIT, 8102);
    }

    @Test
    void storesSiblingEnginesUnderTheirDistinctLogicalAddresses() {
        WorkerStatus first = WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group", "127.0.0.1", 8080, 8081,
                "site", null, 0, 2);
        WorkerStatus second = WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group", "127.0.0.1", 8080, 8081,
                "site", null, 1, 2);

        workerDirectory.currentOrDiscover(
                RoleType.PREFILL, first.getLogicalIpPort(), () -> first);
        workerDirectory.currentOrDiscover(
                RoleType.PREFILL, second.getLogicalIpPort(), () -> second);

        Map<String, WorkerStatus> statuses =
                workerDirectory.getStatusBy(RoleType.PREFILL);
        assertEquals(2, statuses.size());
        assertSame(first, statuses.get("127.0.0.1:8080@0"));
        assertSame(second, statuses.get("127.0.0.1:8080@1"));
    }

    @Test
    void keepsPublishedLogicalEngineWhenSiblingIsNotPublished() {
        WorkerStatus first = WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group", "127.0.0.1", 8080, 8081,
                "site", null, 0, 2);
        WorkerStatus second = WorkerStatus.createDiscovered(
                RoleType.PREFILL, "group", "127.0.0.1", 8080, 8081,
                "site", null, 1, 2);
        discover(first);
        discover(second);
        RunnerTestSupport.publishEndpoint(registry,
                RoleType.PREFILL, first.getLogicalIpPort(), first);

        assertEquals(List.of(first.getLogicalIpPort()),
                workerDirectory.getPrefillCandidates(RoleType.PREFILL)
                        .stream()
                        .map(EndpointRegistry.PrefillRoutingEntry::address)
                        .toList());
    }

    @Test
    void decodeGroupIsRoutableOnlyWhenEveryRankHasAnEndpoint() {
        DecodeMember rankZero = decodeMember("routing-a", "10.0.0.1", 0, 2, 8400);
        DecodeMember rankOne = decodeMember("routing-a", "10.0.0.1", 1, 2, 8401);
        publishDecode(rankZero);

        assertNull(selectDecode(workerDirectory, "routing-a"));
        assertEquals(2, workerDirectory.getWorkerStatuses(RoleType.DECODE, "routing-a").size());

        publishDecode(rankOne);

        assertNotNull(selectDecode(workerDirectory, "routing-a"));
    }

    @Test
    void decodeRoutingEntriesReuseTheRegistryDirectory() {
        publishDecode(decodeMember("routing-a", "10.0.0.1", 0, 2, 8450));
        publishDecode(decodeMember("routing-a", "10.0.0.1", 1, 2, 8451));
        var entries = registry.decodeRoutingEntries();
        assertSame(entries, workerDirectory.decodeRoutingEntries());
    }

    @Test
    void decodeCandidateIsSkippedWhenItsDpAddressChangesBetweenPasses() {
        var firstRead = Mockito.mock(WorkerStatus.EngineObservation.class);
        Mockito.when(firstRead.dpMasterAddress()).thenReturn("10.0.0.1");
        Mockito.when(firstRead.dpSize()).thenReturn(1L);
        var secondRead = Mockito.mock(WorkerStatus.EngineObservation.class);
        Mockito.when(secondRead.dpMasterAddress()).thenReturn("10.0.0.2");
        var status = Mockito.mock(WorkerStatus.class);
        Mockito.when(status.committedEngineObservation()).thenReturn(firstRead, secondRead);
        var endpoint = Mockito.mock(DecodeEndpoint.class);
        Mockito.when(endpoint.getStatus()).thenReturn(status);
        Mockito.when(endpoint.isEmptyForSelection(firstRead)).thenReturn(true);
        var fixedRegistry = Mockito.mock(EndpointRegistry.class);
        Mockito.when(fixedRegistry.decodeRoutingEntries()).thenReturn(List.of(Map.entry("decode", endpoint)));

        assertNull(selectDecode(new WorkerDirectory(fixedRegistry), null));
    }

    @Test
    void retiringOneRankHidesItsGroupWithoutRemovingWorkerStatuses() {
        DecodeMember rankZero = decodeMember("routing-a", "10.0.0.1", 0, 2, 8410);
        DecodeMember rankOne = decodeMember("routing-a", "10.0.0.1", 1, 2, 8411);
        DecodeMember independent = decodeMember("routing-a", null, 0, 1, 8412);
        publishDecode(rankZero);
        publishDecode(rankOne);
        publishDecode(independent);

        EndpointRegistry.DetachedGeneration detached;
        rankOne.status().lock.lock();
        try {
            detached = workerDirectory.beginRetirement(
                    RoleType.DECODE, rankOne.status().getLogicalIpPort(), rankOne.status());
        } finally {
            rankOne.status().lock.unlock();
        }
        assertNotNull(detached);
        detached.retireAndAwait();

        assertSame(independent.status(), selectDecode(workerDirectory, "routing-a").getStatus());
        assertEquals(3, workerDirectory.getWorkerStatuses(RoleType.DECODE, "routing-a").size());
        assertSame(rankOne.status(), workerDirectory.getStatusBy(RoleType.DECODE)
                .get(rankOne.status().getLogicalIpPort()));
    }

    @Test
    void decodeGroupsWithTheSameRanksAreSeparatedByMasterAddress() {
        DecodeMember incomplete = decodeMember("routing-a", "10.0.0.1", 0, 2, 8420);
        DecodeMember completeZero = decodeMember("routing-a", "10.0.0.2", 0, 2, 8421);
        DecodeMember completeOne = decodeMember("routing-a", "10.0.0.2", 1, 2, 8422);
        DecodeMember otherPolicy = decodeMember("routing-b", null, 0, 1, 8423);
        publishDecode(incomplete);
        publishDecode(completeZero);
        publishDecode(completeOne);
        publishDecode(otherPolicy);

        assertTrue(List.of(completeZero.status(), completeOne.status())
                .contains(selectDecode(workerDirectory, "routing-a").getStatus()));
        assertSame(otherPolicy.status(), selectDecode(workerDirectory, "routing-b").getStatus());
        assertNull(selectDecode(workerDirectory, "missing-group"));
    }

    @Test
    void duplicateRanksDoNotMakeAGroupRoutable() {
        publishDecode(decodeMember("routing-a", "10.0.0.1", 0, 2, 8430));
        publishDecode(decodeMember("routing-a", "10.0.0.1", 1, 2, 8431));
        publishDecode(decodeMember("routing-a", "10.0.0.1", 1, 2, 8432));

        assertNull(selectDecode(workerDirectory, "routing-a"));
    }

    @Test
    void inconsistentDpSizesDoNotMakeAGroupRoutable() {
        publishDecode(decodeMember("routing-a", "10.0.0.1", 0, 2, 8440));
        publishDecode(decodeMember("routing-a", "10.0.0.1", 1, 3, 8441));
        publishDecode(decodeMember("routing-a", "10.0.0.1", 2, 3, 8442));

        assertNull(selectDecode(workerDirectory, "routing-a"));
    }

    private static DecodeEndpoint selectDecode(WorkerDirectory directory, String group) {
        var request = new Request();
        request.setRequestId("decode-directory-test");
        var context = new BalanceContext(SchedulingTestConfig.newConfig());
        context.setRequest(request);
        var selection = new DecodeSelector(directory).select(context, group);
        return selection == null ? null : selection.endpoint();
    }

    @Test
    void prefillGroupIsRoutableOnlyWhenEveryRankHasAnEndpoint() {
        PrefillMember rankZero = prefillMember("routing-a", "10.0.1.1", 0, 2, 8500);
        PrefillMember rankOne = prefillMember("routing-a", "10.0.1.1", 1, 2, 8501);
        publishPrefill(rankZero);

        assertTrue(workerDirectory.getPrefillCandidates(RoleType.PREFILL).isEmpty());
        assertEquals(2, workerDirectory.getWorkerStatuses(RoleType.PREFILL, "routing-a").size());

        publishPrefill(rankOne);

        assertEquals(2, workerDirectory.getPrefillCandidates(RoleType.PREFILL).size());
    }

    @Test
    void retiringOnePrefillRankRequiresTheWholeGroupToBeRepublished() {
        PrefillMember rankZero = prefillMember("routing-a", "10.0.1.2", 0, 2, 8510);
        PrefillMember rankOne = prefillMember("routing-a", "10.0.1.2", 1, 2, 8511);
        PrefillMember independent = prefillMember("routing-a", null, 0, 1, 8512);
        publishPrefill(rankZero);
        publishPrefill(rankOne);
        publishPrefill(independent);

        assertEquals(3, workerDirectory.getPrefillCandidates(RoleType.PREFILL).size());

        EndpointRegistry.DetachedGeneration detached;
        rankOne.status().lock.lock();
        try {
            detached = workerDirectory.beginRetirement(
                    RoleType.PREFILL, rankOne.status().getLogicalIpPort(), rankOne.status());
        } finally {
            rankOne.status().lock.unlock();
        }
        assertNotNull(detached);
        workerDirectory.completeRetirement(
                RoleType.PREFILL, rankOne.status().getLogicalIpPort(), rankOne.status(),
                detached, Mockito.mock(CacheAwareService.class),
                LoggerFactory.getLogger(WorkerDirectoryTest.class));

        assertEquals(List.of(independent.status().getLogicalIpPort()),
                workerDirectory.getPrefillCandidates(RoleType.PREFILL)
                        .stream()
                        .map(EndpointRegistry.PrefillRoutingEntry::address)
                        .toList());

        publishPrefill(prefillMember("routing-a", "10.0.1.2", 1, 2, 8511));

        assertEquals(1, workerDirectory.getPrefillCandidates(RoleType.PREFILL).size());

        publishPrefill(prefillMember("routing-a", "10.0.1.2", 0, 2, 8510));

        assertEquals(3, workerDirectory.getPrefillCandidates(RoleType.PREFILL).size());
    }

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {"PREFILL", "DECODE", "PDFUSION"})
    void retiresEveryDpMemberBeforeDrainingAndKeepsOtherGroups(RoleType role) {
        WorkerStatus first = publishDpMember(role, "10.0.2.1", 0, 3, 8600);
        WorkerStatus second = publishDpMember(role, "10.0.2.1", 1, 3, 8601);
        WorkerStatus third = publishDpMember(role, "10.0.2.1", 2, 3, 8602);
        WorkerStatus otherGroup = publishDpMember(role, "10.0.2.2", 0, 1, 8603);
        WorkerStatus independent = publishDpMember(role, null, 0, 1, 8604);
        WorkerStatus otherRole =
                publishDpMember(role == RoleType.DECODE ? RoleType.PREFILL : RoleType.DECODE, "10.0.2.1", 0, 1, 8605);
        CacheAwareService cache = Mockito.mock(CacheAwareService.class);

        EndpointRegistry.DetachedGeneration detached;
        first.lock.lock();
        try {
            detached = Mockito.spy(workerDirectory.beginRetirement(role, first.getLogicalIpPort(), first));
        } finally {
            first.lock.unlock();
        }
        AtomicBoolean groupRetiringBeforeDrain = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            groupRetiringBeforeDrain.set(List.of(first, second, third)
                    .stream()
                    .allMatch(member -> !member.isActiveGeneration()
                            && workerDirectory.getEndpoint(role, member.getLogicalIpPort(), member) == null
                            && workerDirectory.getStatusBy(role).get(member.getLogicalIpPort()) == member
                            && !member.lock.isHeldByCurrentThread()));
            return invocation.callRealMethod();
        }).when(detached).retireAndAwait();

        workerDirectory.completeRetirement(role, first.getLogicalIpPort(), first, detached, cache,
                LoggerFactory.getLogger(WorkerDirectoryTest.class));

        assertTrue(groupRetiringBeforeDrain.get());
        for (WorkerStatus member : List.of(first, second, third)) {
            assertNull(workerDirectory.findEndpoint(role, member.getLogicalIpPort()));
        }
        assertEquals(Map.of(otherGroup.getLogicalIpPort(), otherGroup, independent.getLogicalIpPort(), independent),
                workerDirectory.getStatusBy(role));
        assertEquals(2, workerDirectory.routingCapacity(role));
        assertTrue(otherRole.isActiveGeneration());
        assertNotNull(workerDirectory.getEndpoint(otherRole.getRole(), otherRole.getLogicalIpPort(), otherRole));
        if (role.requiresCacheKeys()) {
            for (WorkerStatus member : List.of(first, second, third)) {
                Mockito.verify(cache).removeEngineBlockCache(member.getLogicalIpPort());
            }
            Mockito.verifyNoMoreInteractions(cache);
        } else {
            Mockito.verifyNoInteractions(cache);
        }
    }

    private WorkerStatus publishDpMember(RoleType role, String masterAddress, long rank, long size, int port) {
        WorkerStatus status = status(role, "routing-a", port);
        discover(status);
        WorkerStatusPB.Builder response = RunnerTestSupport.response(status, true, 1L);
        if (masterAddress != null) {
            response.setDpMasterAddress(masterAddress);
        }
        response.setDpRank(rank);
        response.setDpSize((int) size);
        status.lock.lock();
        try {
            registry.publishPreparedEndpoint(status.getLogicalIpPort(), status, status
                    .prepareNewStatus(EngineStatusConverter.convertToStatusObservation(status, response.build())));
            status.recordSuccessfulPoll(true);
        } finally {
            status.lock.unlock();
        }
        return status;
    }

    @Test
    void prefillGroupsWithTheSameRanksAreSeparatedByMasterAddress() {
        PrefillMember incomplete = prefillMember("routing-a", "10.0.1.3", 0, 2, 8520);
        PrefillMember completeZero = prefillMember("routing-a", "10.0.1.4", 0, 2, 8521);
        PrefillMember completeOne = prefillMember("routing-a", "10.0.1.4", 1, 2, 8522);
        publishPrefill(incomplete);
        publishPrefill(completeZero);
        publishPrefill(completeOne);

        List<String> routableAddresses = workerDirectory.getPrefillCandidates(RoleType.PREFILL)
                .stream()
                .map(EndpointRegistry.PrefillRoutingEntry::address)
                .sorted()
                .toList();
        assertEquals(List.of(completeZero.status().getLogicalIpPort(), completeOne.status().getLogicalIpPort())
                .stream().sorted().toList(), routableAddresses);
    }

    private PrefillMember prefillMember(String group, String masterAddress, long rank, long size, int port) {
        WorkerStatus status = status(RoleType.PREFILL, group, port);
        discover(status);
        WorkerStatusPB.Builder response = RunnerTestSupport.response(status, true, 1L);
        if (masterAddress != null) {
            response.setDpMasterAddress(masterAddress);
        }
        response.setDpSize((int) size);
        response.setDpRank(rank);
        return new PrefillMember(status, response);
    }

    private void publishPrefill(PrefillMember member) {
        WorkerStatus status = member.status();
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared = status.prepareNewStatus(
                    EngineStatusConverter.convertToStatusObservation(status, member.response().build()));
            registry.publishPreparedEndpoint(status.getLogicalIpPort(), status, prepared);
            status.recordSuccessfulPoll(true);
        } finally {
            status.lock.unlock();
        }
    }

    private DecodeMember decodeMember(String group, String masterAddress, long rank, long size, int port) {
        WorkerStatus status = status(RoleType.DECODE, group, port);
        discover(status);
        WorkerStatusPB.Builder response = RunnerTestSupport.response(status, true, 1L);
        if (masterAddress != null) {
            response.setDpMasterAddress(masterAddress);
        }
        response.setDpSize((int) size);
        response.setDpRank(rank);
        return new DecodeMember(status, response);
    }

    private void publishDecode(DecodeMember member) {
        WorkerStatus status = member.status();
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared = status.prepareNewStatus(
                    EngineStatusConverter.convertToStatusObservation(status, member.response().build()));
            registry.publishPreparedEndpoint(status.getLogicalIpPort(), status, prepared);
            status.recordSuccessfulPoll(true);
        } finally {
            status.lock.unlock();
        }
    }

    private void assertRoleEndpoint(RoleType role, int port) {
        WorkerStatus status = status(role, null, port);
        WorkerEndpoint registered = RunnerTestSupport.publishEndpoint(registry,
                role, status.getLogicalIpPort(), status);

        assertSame(registered, workerDirectory.findEndpoint(role, status.getLogicalIpPort()));
    }

    private static WorkerStatus status(
            RoleType role, String group, int port) {
        return RunnerTestSupport.discovered(
                role, group, "127.0.0.1", port, port + 1, "test-site");
    }

    @Test
    void role_status_maps_are_independent_and_counted_once() {
        WorkerStatus prefill = status(RoleType.PREFILL, "group1", 8201);
        WorkerStatus decode = status(RoleType.DECODE, "group2", 8202);
        discover(prefill);
        discover(decode);

        assertSame(prefill, workerDirectory.getStatusBy(RoleType.PREFILL)
                .get(prefill.getLogicalIpPort()));
        assertSame(decode, workerDirectory.getStatusBy(RoleType.DECODE)
                .get(decode.getLogicalIpPort()));
        assertEquals(2, workerDirectory.discoveredCount());
        assertEquals(1, workerDirectory.discoveredCount(RoleType.PREFILL));
    }

    @Test
    void status_view_reflects_newly_discovered_workers() {
        Map<String, WorkerStatus> statuses = workerDirectory.getStatusBy(RoleType.VIT);
        assertTrue(statuses.isEmpty());

        WorkerStatus status = status(RoleType.VIT, "group", 8080);
        discover(status);

        assertEquals(Map.of(status.getLogicalIpPort(), status), statuses);
    }

    @Test
    void worker_status_provider_without_group_returns_live_view() {
        var statuses = workerDirectory.getWorkerStatuses(RoleType.VIT, null);
        assertTrue(statuses.isEmpty());

        WorkerStatus status = status(RoleType.VIT, "group", 8080);
        discover(status);

        assertEquals(1, statuses.size());
        assertTrue(statuses.contains(status));
    }

    @Test
    void worker_status_provider_returns_discovered_statuses_by_role_and_group() {
        WorkerStatus matching = status(RoleType.PREFILL, "group1", 8301);
        WorkerStatus filtered = status(RoleType.PREFILL, "group2", 8302);
        WorkerStatus otherRole = status(RoleType.DECODE, "group1", 8303);
        discover(matching);
        discover(filtered);
        discover(otherRole);

        WorkerStatusProvider provider = workerDirectory;

        assertEquals(List.of(matching),
                provider.getWorkerStatuses(RoleType.PREFILL, "group1"));
        assertEquals(2,
                provider.getWorkerStatuses(RoleType.PREFILL, null).size());
        assertTrue(provider.getWorkerStatuses(null, "group1").isEmpty());
    }

    private void discover(WorkerStatus status) {
        assertSame(status, workerDirectory.currentOrDiscover(
                status.getRole(), status.getLogicalIpPort(), () -> status));
    }
}
