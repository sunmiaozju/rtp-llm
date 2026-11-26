package org.flexlb.dao.master;

import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.KvCacheGroupMode;
import org.flexlb.enums.PriorityPreemptionProgress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 {@link WorkerStatus} 的状态发布契约。
 *
 * <p>测试直接构造状态观测，在 generation 锁内准备并发布状态，验证字段与游标通过 CAS 整体更新，
 * 以及版本递增、退役状态和轮询健康记录的约束。资源可用性判断由资源统计和路由层负责。</p>
 */
@DisplayName("WorkerStatus status-commit contract")
class WorkerStatusTest {

    private static WorkerStatus discovered() {
        return WorkerStatus.createDiscovered(RoleType.PREFILL, "group-a", "10.0.0.1", 8080, 9090, "site-a", null, 0, 1,
                false);
    }

    @Test
    @DisplayName("Discovery identity keeps a physical transport address and logical engine key")
    void discoveredWorkerSeparatesPhysicalAndLogicalIdentity() {
        WorkerStatus status = WorkerStatus.createDiscovered(RoleType.PREFILL, "group-a", "10.0.0.1", 8080, 9090,
                "site-a", "deployment-a", 1, 2, false);

        assertEquals("10.0.0.1:8080", status.getIpPort());
        assertEquals("10.0.0.1:8080", status.getPhysicalIpPort());
        assertEquals("10.0.0.1:8080@1", status.getLogicalIpPort());
        assertEquals("10.0.0.1:8080@1", status.getMetricIpPort());
        assertEquals(1, status.getEngineIndex());
        assertEquals(2, status.getMultiEngineNum());
    }

    @Test
    @DisplayName("Single-engine workers retain their physical metric identity")
    void singleEngineWorkerUsesPhysicalMetricIdentity() {
        WorkerStatus status = WorkerStatus.createDiscovered(RoleType.PREFILL, "group-a", "10.0.0.1", 8080, 9090,
                "site-a", "deployment-a", 0, 1, false);

        assertEquals("10.0.0.1:8080", status.getMetricIpPort());
    }

    /**
     * 在 generation 锁内准备并发布完整的状态观测。
     */
    private static void publish(WorkerStatus status, WorkerStatus.StatusObservation observation) {
        status.lock.lock();
        try {
            WorkerStatus.PreparedStatus prepared =
                    status.prepareNewStatus(observation);
            status.publishPreparedStatus(prepared);
        } finally {
            status.lock.unlock();
        }
    }

    /**
     * 每个 Engine 字段都由 marker 决定，用于检测字段与游标是否属于同一轮发布。
     */
    private static WorkerStatus.StatusObservation observationWithMarker(WorkerStatus status, long marker) {
        return observationWithMarker(status, marker, marker);
    }

    private static WorkerStatus.StatusObservation observationWithMarker(WorkerStatus status,
                                                                        long marker,
                                                                        long finishedVersion) {
        String requestId = Long.toString(marker);
        WorkerStatus.TaskObservation task = new WorkerStatus.TaskObservation(requestId, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                null, -1L, null, -1L, PriorityPreemptionProgress.NONE, false, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, null, null, null);
        WorkerStatus.EngineObservation engine = new WorkerStatus.EngineObservation(RoleType.PREFILL, marker,
                100L + marker, 1_000L + marker, Map.of(requestId, task), marker + 0.25, marker, 10L + marker,
                20L + marker, 30L + marker, "master-" + marker, 0L, 0, 0, KvCacheGroupMode.UNSPECIFIED, 40L + marker,
                50L + marker, 0L, 0L, null);
        return status.bindStatusObservation(engine, true, marker, finishedVersion, Map.of());
    }

    private static void assertCoherent(WorkerStatus.CommittedWorkerStatus committed) {
        WorkerStatus.EngineObservation fields = committed.fields();
        long marker = fields.iterateCount();
        assertTrue(marker >= 1L, "marker must be a published version");
        // Fields and cursor are one immutable record swapped atomically; every
        // field must belong to the same marker as the committed cursor.
        assertEquals(marker, committed.cursor().statusVersion());
        assertEquals(marker, fields.availableConcurrency().longValue());
        assertEquals(100L + marker, fields.availableKvCacheTokens());
        assertEquals(1_000L + marker, fields.totalKvCacheTokens());
        assertEquals(marker + 0.25, fields.stepLatencyMs());
        assertEquals(10L + marker, fields.dpSize());
        assertEquals(20L + marker, fields.tpSize());
        assertEquals(30L + marker, fields.dpRank());
        assertEquals("master-" + marker, fields.dpMasterAddress());
        assertEquals(40L + marker, fields.maxSeqLen());
        assertEquals(50L + marker, fields.maxBatchTokensSize());
        assertEquals(Long.toString(marker),
                fields.runningTaskList().get(Long.toString(marker)).requestId());
    }

    @Nested
    @DisplayName("Atomic publication")
    class AtomicPublication {

        @Test
        @DisplayName("Readers never observe a torn field/cursor pair")
        void readersObserveOneWholePublishedSnapshot() throws Exception {
            WorkerStatus status = discovered();
            publish(status, observationWithMarker(status, 1L));

            int readers = 4;
            int observationsPerReader = 20_000;
            AtomicLong nextVersion = new AtomicLong(2L);
            CountDownLatch ready = new CountDownLatch(readers + 1);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(readers + 1);
            List<Future<?>> futures = new ArrayList<>();
            try {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    for (int i = 0; i < observationsPerReader; i++) {
                        publish(status, observationWithMarker(status, nextVersion.getAndIncrement()));
                    }
                    return null;
                }));
                for (int reader = 0; reader < readers; reader++) {
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        assertTrue(start.await(5, TimeUnit.SECONDS));
                        for (int i = 0; i < observationsPerReader; i++) {
                            assertCoherent(status.committedWorkerStatus());
                        }
                        return null;
                    }));
                }

                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Nested
    @DisplayName("Prepare/publish transaction invariants")
    class TransactionInvariants {

        @Test
        @DisplayName("prepareNewStatus rejects a non-advancing status version")
        void prepareRejectsNonStrictlyNewerVersion() {
            WorkerStatus status = discovered();
            publish(status, observationWithMarker(status, 5L));

            WorkerStatus.StatusObservation stale = observationWithMarker(status, 5L); // equal version
            status.lock.lock();
            try {
                WorkerStatus.StatusObservation observation = stale;
                assertThrows(IllegalArgumentException.class,
                        () -> status.prepareNewStatus(observation));
            } finally {
                status.lock.unlock();
            }
        }

        @Test
        @DisplayName("prepareNewStatus requires the generation lock")
        void prepareRequiresGenerationLock() {
            WorkerStatus status = discovered();
            WorkerStatus.StatusObservation observation = observationWithMarker(status, 1L);
            assertThrows(IllegalStateException.class,
                    () -> status.prepareNewStatus(observation));
        }

        @Test
        @DisplayName("publishPreparedStatus requires the generation lock")
        void publishRequiresGenerationLock() {
            WorkerStatus status = discovered();
            WorkerStatus.PreparedStatus prepared;
            status.lock.lock();
            try {
                prepared = status.prepareNewStatus(observationWithMarker(status, 1L));
            } finally {
                status.lock.unlock();
            }
            assertThrows(IllegalStateException.class,
                    () -> status.publishPreparedStatus(prepared));
        }

        @Test
        @DisplayName("A stale prepared status loses the publish CAS")
        void stalePreparedStatusIsRejectedByCas() {
            WorkerStatus status = discovered();
            publish(status, observationWithMarker(status, 1L));

            // Two transactions prepared from the same committed base, mirroring
            // two overlapping status rounds. Only the first publish may win.
            WorkerStatus.PreparedStatus first;
            WorkerStatus.PreparedStatus second;
            status.lock.lock();
            try {
                first = status.prepareNewStatus(observationWithMarker(status, 2L));
                second = status.prepareNewStatus(observationWithMarker(status, 3L));
            } finally {
                status.lock.unlock();
            }

            status.lock.lock();
            try {
                status.publishPreparedStatus(first);
                assertThrows(IllegalStateException.class,
                        () -> status.publishPreparedStatus(second));
            } finally {
                status.lock.unlock();
            }
            assertEquals(2L, status.appliedStatusCursor().statusVersion());
        }

        @Test
        @DisplayName("A retiring generation refuses further publication")
        void retiringGenerationRefusesPublication() {
            WorkerStatus status = discovered();
            publish(status, observationWithMarker(status, 1L));

            status.lock.lock();
            try {
                assertTrue(status
                        .beginRetirementAfterEndpointGateClosed());
                WorkerStatus.StatusObservation observation = observationWithMarker(status, 2L);
                assertThrows(IllegalStateException.class,
                        () -> status.prepareNewStatus(observation));
            } finally {
                status.lock.unlock();
            }
            assertFalse(status.isActiveGeneration());
        }
    }

    @Nested
    @DisplayName("Cursor merge")
    class CursorMerge {

        @Test
        @DisplayName("Finished-task version never regresses across publishes")
        void finishedVersionIsMonotonic() {
            WorkerStatus status = discovered();
            long[] finishedVersions = {5L, 3L, 10L, 2L};
            long[] expectedHighWater = {5L, 5L, 10L, 10L};
            for (int round = 0; round < finishedVersions.length; round++) {
                publish(status, observationWithMarker(status, round + 1L, finishedVersions[round]));
                assertEquals(expectedHighWater[round],
                        status.appliedStatusCursor().latestFinishedTaskVersion());
            }
        }

        @Test
        @DisplayName("Serialized publishers advance the cursor exactly once each")
        void serializedPublishersAdvanceCursorMonotonically() throws Exception {
            WorkerStatus status = discovered();
            int publishers = 32;
            AtomicInteger version = new AtomicInteger(1);
            CountDownLatch ready = new CountDownLatch(publishers);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(publishers);
            List<Future<?>> futures = new ArrayList<>(publishers);
            try {
                for (int i = 0; i < publishers; i++) {
                    futures.add(executor.submit(() -> {
                        ready.countDown();
                        assertTrue(start.await(5, TimeUnit.SECONDS));
                        status.lock.lock();
                        try {
                            long next = version.getAndIncrement();
                            WorkerStatus.StatusObservation observation = observationWithMarker(status, next);
                            status.publishPreparedStatus(
                                    status.prepareNewStatus(observation));
                        } finally {
                            status.lock.unlock();
                        }
                        return null;
                    }));
                }
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(5, TimeUnit.SECONDS);
                }
                assertEquals(publishers,
                        status.appliedStatusCursor().statusVersion());
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Nested
    @DisplayName("Generation identity and topology")
    class GenerationIdentity {

        @Test
        @DisplayName("Each discovered generation has a distinct identity")
        void generationsHaveDistinctIdentity() {
            WorkerStatus first = discovered();
            WorkerStatus second = discovered();
            assertNotEquals(first.getGenerationId(), second.getGenerationId());
        }

        @Test
        @DisplayName("Discovery labels are refreshed under the generation lock")
        void discoveryLabelsHaveExplicitPublication() {
            WorkerStatus status = discovered();
            status.lock.lock();
            try {
                status.updateDiscoveryLabels("site-b", "group-b");
            } finally {
                status.lock.unlock();
            }
            WorkerStatus.TopologySnapshot topology = status.topologySnapshot();
            assertEquals("site-b", topology.site());
            assertEquals("group-b", topology.group());
            // Address identity is discovery-owned and must survive a label update.
            assertEquals("10.0.0.1", topology.ip());
            assertEquals(8080, topology.port());
        }

        @Test
        @DisplayName("updateDiscoveryLabels requires the generation lock")
        void discoveryLabelUpdateRequiresLock() {
            WorkerStatus status = discovered();
            assertThrows(IllegalStateException.class,
                    () -> status.updateDiscoveryLabels("site-x", "group-x"));
        }
    }

    @Nested
    @DisplayName("Poll health")
    class PollHealthContract {

        @Test
        @DisplayName("A successful poll publishes reported liveness")
        void successfulPollRecordsLiveness() {
            WorkerStatus status = discovered();
            status.lock.lock();
            try {
                WorkerStatus.PollHealth health = status.recordSuccessfulPoll(true);
                assertTrue(health.reportedAlive());
                assertEquals(0L, health.consecutiveTransportFailures());
            } finally {
                status.lock.unlock();
            }
        }

        @Test
        @DisplayName("Transport failures accumulate without rewriting liveness")
        void transportFailuresAccumulate() {
            WorkerStatus status = discovered();
            status.lock.lock();
            try {
                status.recordSuccessfulPoll(true);
                WorkerStatus.PollHealth afterFirst = status.recordTransportFailure();
                WorkerStatus.PollHealth afterSecond = status.recordTransportFailure();
                assertEquals(1L, afterFirst.consecutiveTransportFailures());
                assertEquals(2L, afterSecond.consecutiveTransportFailures());
                assertTrue(afterSecond.reportedAlive());
            } finally {
                status.lock.unlock();
            }
            assertSame(status.pollHealth(), status.pollHealth());
        }
    }
}
