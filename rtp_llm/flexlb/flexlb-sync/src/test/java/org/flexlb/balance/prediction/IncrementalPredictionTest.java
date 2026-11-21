package org.flexlb.balance.prediction;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

}
