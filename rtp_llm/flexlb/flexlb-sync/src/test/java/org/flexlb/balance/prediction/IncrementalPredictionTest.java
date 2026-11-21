package org.flexlb.balance.prediction;

import org.flexlb.balance.planner.GroupPlanner;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class IncrementalPredictionTest {
    static String formula() throws Exception {
        try (var stream = IncrementalPredictionTest.class.getResourceAsStream("/prediction/deepseek-v4.txt")) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void everyPrefixIsBitExactForRealFormulaAndNumericEdges() throws Exception {
        for (String expression : List.of(formula(),
                "sum(computeTokens) + totalComputeTokens^2 + maxInputTokens + maxComputeTokens + batchSize",
                "sum(sum(computeTokens / 3)) + sum(max(hitCacheTokens - 4, 0))",
                "inputTokens + computeTokens + hasHitCache + hitCacheTokens", // scalar bindings remain zero in batches
                "-0.0", "sum(-0.0)", "sum(computeTokens / 0)",
                "sum(1e308 * computeTokens) - sum(1e308 * computeTokens)")) {
            var model = new FormulaPredictor(expression);
            Random random = new Random(41);
            for (int trial = 0; trial < 100; trial++) {
                var incremental = model.newBatchPrediction();
                List<PrefillBatchFeatures.Item> prefix = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    long input = trial == 0 ? new long[]{0, 1, (1L << 53) + 1, Long.MAX_VALUE}[i % 4]
                            : random.nextInt(131072);
                    long hit = input == 0 ? 0 : Math.floorMod(random.nextLong(), input);
                    prefix.add(new PrefillBatchFeatures.Item(input, hit));
                    assertEquals(Double.doubleToLongBits(model.predictBatchMs(new PrefillBatchFeatures(prefix))),
                            Double.doubleToLongBits(incremental.append(input, hit)),
                            "expression=" + expression + " prefix=" + prefix.size());
                }
            }
        }
    }

    @Test
    void freshSessionsHandleRemovalInsertionReorderAndModelReplacement() throws Exception {
        var random = new Random(73);
        var source = new ArrayList<GroupPlanner.Item>();
        var model = new FormulaPredictor(formula());
        for (int i = 0; i < 64; i++) source.add(item(i, 100 + random.nextInt(32768), i % 4, 100_000));
        for (int round = 0; round < 120; round++) {
            if (round % 3 == 0 && !source.isEmpty()) source.remove(random.nextInt(source.size()));
            else source.add(random.nextInt(source.size() + 1), item(1000 + round, 1000, 0, 100_000));
            if (round % 5 == 0) java.util.Collections.reverse(source);
            var constraints = new GroupPlanner.Constraints(64, 200_000, 300_000, 700, 700);
            var full = GroupPlanner.select(source, GroupPlanner.itemAccess(), constraints,
                    items -> model.predictBatchMs(PrefillBatchFeatures.from(items,
                            GroupPlanner.Item::seqLen, GroupPlanner.Item::hitCache)));
            var batch = model.newBatchPrediction();
            var incremental = GroupPlanner.selectWithPrediction(source, GroupPlanner.itemAccess(), constraints,
                    (added, prefix) -> batch.append(added.seqLen(), added.hitCache()));
            assertEquals(full, incremental);
            assertEquals(GroupPlanner.evaluateReadiness(full, constraints, 2000),
                    GroupPlanner.evaluateReadiness(incremental, constraints, 2000));
        }
        var oldSession = model.newBatchPrediction();
        var replacement = new FormulaPredictor("42");
        assertNotEquals(oldSession.append(100, 0), replacement.newBatchPrediction().append(100, 0));
    }

    @Test
    void concurrentSessionsDoNotShareMutableAccumulators() throws Exception {
        var model = new FormulaPredictor(formula());
        try (var pool = Executors.newFixedThreadPool(8)) {
            var tasks = new ArrayList<java.util.concurrent.Callable<Double>>();
            for (int k = 0; k < 32; k++) {
                int count = k + 1;
                tasks.add(() -> {
                    var batch = model.newBatchPrediction();
                    double actual = 0;
                    var items = new ArrayList<PrefillBatchFeatures.Item>();
                    for (int i = 0; i < count; i++) {
                        items.add(new PrefillBatchFeatures.Item(1000, 333));
                        actual = batch.append(1000, 333);
                    }
                    assertEquals(model.predictBatchMs(new PrefillBatchFeatures(items)), actual);
                    return actual;
                });
            }
            for (var result : pool.invokeAll(tasks)) result.get();
        }
    }

    private static GroupPlanner.Item item(long id, long input, int priority, long expires) {
        return new GroupPlanner.Item(Long.toString(id), priority, id, 1000, expires, input, input / 3);
    }
}
