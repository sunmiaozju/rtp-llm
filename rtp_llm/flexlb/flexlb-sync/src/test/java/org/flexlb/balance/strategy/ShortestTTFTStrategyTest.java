package org.flexlb.balance.strategy;

import org.flexlb.cache.service.CacheAwareService;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.loadbalance.MasterRequest;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.CacheStatus;
import org.flexlb.dao.master.TaskInfo;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.domain.balance.BalanceContext;
import org.flexlb.domain.balance.WhaleMasterConfig;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.sync.status.EngineWorkerStatus;
import org.flexlb.sync.status.ModelWorkerStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ShortestTTFTStrategyTest {

  private CacheAwareService cacheAwareService;
  private ShortestTTFTStrategy strategy;

  @BeforeEach
  void setUp() {
    EngineWorkerStatus engineWorkerStatus = new EngineWorkerStatus(new ModelMetaConfig());
    EngineHealthReporter engineHealthReporter = Mockito.mock(EngineHealthReporter.class);
    cacheAwareService = Mockito.mock(CacheAwareService.class);
    strategy =
        new ShortestTTFTStrategy(engineWorkerStatus, engineHealthReporter, cacheAwareService);
  }

  @Test
  @DisplayName("测试选择TTFT最短的Worker")
  void testSelectWorkerWithShortestTTFT() {
    setupModelWorkers();

    MasterRequest request = createMasterRequest();
    BalanceContext balanceContext = createBalanceContext(request);

    ServerStatus result = strategy.select(balanceContext, RoleType.PREFILL, null);

    Assertions.assertTrue(result.isSuccess());
    Assertions.assertEquals("127.0.0.2", result.getServerIp());
    Assertions.assertEquals(8080, result.getHttpPort());
  }

  @Test
  @DisplayName("测试无可用Worker时返回错误")
  void testSelectWithNoAvailableWorkers() {
    EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.put("test-model", new ModelWorkerStatus());

    MasterRequest request = createMasterRequest();
    BalanceContext balanceContext = createBalanceContext(request);

    ServerStatus result = strategy.select(balanceContext, RoleType.PREFILL, null);

    Assertions.assertFalse(result.isSuccess());
  }

  @Test
  @DisplayName("测试所有Worker不存活时返回错误")
  void testSelectWithAllWorkersDown() {
    setupModelWorkers();
    Map<String, WorkerStatus> prefillStatusMap =
        EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.get("test-model").getPrefillStatusMap();

    prefillStatusMap.values().forEach(worker -> worker.setAlive(false));

    MasterRequest request = createMasterRequest();
    BalanceContext balanceContext = createBalanceContext(request);

    ServerStatus result = strategy.select(balanceContext, RoleType.PREFILL, null);

    Assertions.assertFalse(result.isSuccess());
  }

  @Test
  @DisplayName("测试缓存命中时优先选择高命中率Worker")
  void testSelectWorkerWithCacheHit() {
    setupModelWorkers();

    Map<String, Integer> cacheMatchResults = new HashMap<>();
    cacheMatchResults.put("127.0.0.1:8080", 10);
    cacheMatchResults.put("127.0.0.2:8080", 5);

    Mockito.when(
            cacheAwareService.findMatchingEngines(
                Mockito.anyList(),
                Mockito.eq("test-model"),
                Mockito.eq(RoleType.PREFILL),
                Mockito.isNull()))
        .thenReturn(cacheMatchResults);

    MasterRequest request = createMasterRequest();
    BalanceContext balanceContext = createBalanceContext(request);

    ServerStatus result = strategy.select(balanceContext, RoleType.PREFILL, null);

    Assertions.assertTrue(result.isSuccess());
  }

  @Test
  @DisplayName("测试释放本地缓存")
  void testReleaseLocalCache() {
    setupModelWorkers();

    Map<String, WorkerStatus> prefillStatusMap =
        EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.get("test-model").getPrefillStatusMap();
    WorkerStatus worker = prefillStatusMap.get("127.0.0.1:8080");

    TaskInfo task = new TaskInfo();
    task.setInterRequestId(12345L);
    worker.putLocalTask(12345L, task);

    Assertions.assertEquals(1, worker.getLocalTaskMap().size());

    strategy.releaseLocalCache("test-model", "127.0.0.1:8080", 12345L);

    Assertions.assertEquals(0, worker.getLocalTaskMap().size());
  }

  @Test
  @DisplayName("测试多个Worker时的负载均衡")
  void testLoadBalancingWithMultipleWorkers() {
    setupMultipleWorkers();

    MasterRequest request = createMasterRequest();
    BalanceContext balanceContext = createBalanceContext(request);

    ServerStatus result = strategy.select(balanceContext, RoleType.PREFILL, null);

    Assertions.assertTrue(result.isSuccess());
    Assertions.assertNotNull(result.getServerIp());
  }

  private void setupModelWorkers() {
    EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.put("test-model", new ModelWorkerStatus());
    Map<String, WorkerStatus> prefillStatusMap =
        EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.get("test-model").getPrefillStatusMap();

    WorkerStatus worker1 = createWorkerStatus("127.0.0.1", 200);
    WorkerStatus worker2 = createWorkerStatus("127.0.0.2", 100);

    prefillStatusMap.put("127.0.0.1:8080", worker1);
    prefillStatusMap.put("127.0.0.2:8080", worker2);
  }

  private void setupMultipleWorkers() {
    EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.put("test-model", new ModelWorkerStatus());
    Map<String, WorkerStatus> prefillStatusMap =
        EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS_MAP.get("test-model").getPrefillStatusMap();

    for (int i = 1; i <= 5; i++) {
      WorkerStatus worker = createWorkerStatus("127.0.0." + i, i * 50L);
      prefillStatusMap.put("127.0.0." + i + ":8080", worker);
    }
  }

  private WorkerStatus createWorkerStatus(String ip, long runningQueueTime) {
    WorkerStatus workerStatus = new WorkerStatus();
    workerStatus.setIp(ip);
    workerStatus.setPort(8080);
    workerStatus.setSite("na61");
    workerStatus.setAlive(true);

    CacheStatus cacheStatus = new CacheStatus();
    cacheStatus.setAvailableKvCache(10000);
    cacheStatus.setBlockSize(128);
    workerStatus.setCacheStatus(cacheStatus);

    workerStatus.getRunningQueueTime().set(runningQueueTime);
    workerStatus.setRunningTaskList(new ArrayList<>());
    workerStatus.setLocalTaskMap(new ConcurrentHashMap<>());

    return workerStatus;
  }

  private MasterRequest createMasterRequest() {
    MasterRequest request = new MasterRequest();
    request.setModel("test-model");
    request.setSeqLen(1000);

    List<Long> blockCacheKeys = new ArrayList<>();
    blockCacheKeys.add(1L);
    blockCacheKeys.add(2L);
    request.setBlockCacheKeys(blockCacheKeys);

    return request;
  }

  private BalanceContext createBalanceContext(MasterRequest request) {
    BalanceContext balanceContext = new BalanceContext();
    balanceContext.setConfig(new WhaleMasterConfig());
    balanceContext.setMasterRequest(request);
    balanceContext.setWorkerCalcParallel(4);
    balanceContext.setInterRequestId(System.currentTimeMillis());
    return balanceContext;
  }
}
