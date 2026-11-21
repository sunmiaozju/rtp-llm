package org.flexlb.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlexlbConfigSchedulingModeTest {

    @Test
    void direct_scheduler_type_is_rejected() {
        var error = assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                {
                  "scheduler":{"type":"DIRECT"},
                  "dispatcher":{"type":"NON_BATCH_V2"}
                }
                """));

        assertTrue(error.getMessage().contains("scheduler.type"), error.getMessage());
        assertTrue(error.getMessage().contains("DIRECT is not supported"), error.getMessage());
    }

    @Test
    void omitted_queue_fields_use_canonical_scheduler_defaults() {
        FlexlbConfig defaults = ConfigTestFixtures.parse("""
                {
                  "scheduler":{"type":"QUEUE","ordering":{"type":"FIFO"}}
                }
                """);
        assertEquals(DispatcherConfig.Type.NON_BATCH_V2, defaults.getDispatcher().getType());
        assertEquals(2, defaults.getDispatcher().getMaxInflightPerPrefillWorker());
        assertEquals(4, defaults.getDispatcher().getPrefillSubqueueLimitMultiplier());
    }

    @Test
    void non_batch_route_queue_capacity_is_configurable_without_a_mode_switch() {
        FlexlbConfig config = ConfigTestFixtures.parse("""
                {"scheduler":{"type":"QUEUE"},"dispatcher":{"type":"NON_BATCH_V2",
                 "prefillSubqueueLimitMultiplier":8}}
                """);
        assertEquals(8, config.getDispatcher().getPrefillSubqueueLimitMultiplier());
        assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                {"dispatcher":{"prefillSubqueueLimitMultiplier":0}}
                """));
    }

    @Test
    void queue_capacity_is_positive() {
        assertEquals(100_000, ConfigTestFixtures.parse("{}").queueScheduler().getMaxQueuedRequests());
        assertEquals(2, ConfigTestFixtures.parse("""
                {"scheduler":{"type":"QUEUE","maxQueuedRequests":2}}
                """).queueScheduler().getMaxQueuedRequests());
        for (String value : new String[]{"0", "-1", "\"2\"", "null"}) {
            var error = assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                    {"scheduler":{"type":"QUEUE","maxQueuedRequests":%s}}
                    """.formatted(value)));
            assertTrue(error.getMessage().contains("maxQueuedRequests"), error.getMessage());
        }
    }

    @Test
    void high_priority_threshold_is_configurable_in_priority_ordering() {
        assertEquals(50, ConfigTestFixtures.parse("""
                {"scheduler":{"ordering":{"type":"PRIORITY"}}}
                """).priorityOrdering().getHighPriorityThreshold());
        assertEquals(70, ConfigTestFixtures.parse("""
                {"scheduler":{"ordering":{"type":"PRIORITY","highPriorityThreshold":70}}}
                """).priorityOrdering().getHighPriorityThreshold());
        for (String value : new String[]{"0", "101", "-1", "null", "\"50\""}) {
            var error = assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                    {"scheduler":{"ordering":{"type":"PRIORITY","highPriorityThreshold":%s}}}
                    """.formatted(value)));
            assertTrue(error.getMessage().contains("highPriorityThreshold"), error.getMessage());
        }
        var error = assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                {"scheduler":{"ordering":{"type":"FIFO","highPriorityThreshold":50}}}
                """));
        assertTrue(error.getMessage().contains("scheduler.ordering.highPriorityThreshold"));
    }

    @Test
    void known_fifo_fields_and_queue_limits_still_require_valid_values() {
        assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                {
                  "scheduler":{"type":"QUEUE","ordering":{"type":"FIFO","defaultPriority":50}},
                  "dispatcher":{"type":"NON_BATCH_V2"}
                }
                """));
        assertThrows(ConfigValidationException.class, () -> ConfigTestFixtures.parse("""
                {
                  "scheduler":{"type":"QUEUE","queueTimeoutMs":0,
                    "ordering":{"type":"FIFO"}},
                  "dispatcher":{"type":"NON_BATCH_V2"}
                }
                """));
    }

    @Test
    void all_delivery_modes_share_the_same_default_and_explicit_limit() {
        for (String mode : new String[]{"QUEUE/NON_BATCH_V2"}) {
            String[] parts = mode.split("/");
            FlexlbConfig config = parse(parts[0], parts[1], "");
            assertEquals(2, config.getDispatcher().getMaxInflightPerPrefillWorker(), mode);
            for (int limit : new int[]{1, 7, Integer.MAX_VALUE}) {
                config = parse(parts[0], parts[1], ",\"maxInflightPerPrefillWorker\":" + limit);
                assertEquals(limit, config.getDispatcher().getMaxInflightPerPrefillWorker(), mode);
            }
        }
    }

    @Test
    void encoder_limit_is_optional_and_requires_a_positive_integer() {
        assertEquals(Integer.MAX_VALUE,
                parse("QUEUE", "NON_BATCH_V2", "").getDispatcher().getMaxInflightPerEncoderWorker());
        assertEquals(3,
                parse("QUEUE", "NON_BATCH_V2", ",\"maxInflightPerEncoderWorker\":3")
                        .getDispatcher().getMaxInflightPerEncoderWorker());
        for (String value : new String[]{"0", "-1", "1.5", "\"2\"", "true", "null", "[]", "{}"}) {
            assertThrows(ConfigValidationException.class,
                    () -> parse("QUEUE", "NON_BATCH_V2", ",\"maxInflightPerEncoderWorker\":" + value), value);
        }
    }

    @Test
    void limits_require_positive_integers_without_json_coercion() {
        for (String type : new String[]{"NON_BATCH_V2"}) {
            for (String value : new String[]{"0", "-1", "1.5", "\"2\"", "true", "null", "[]", "{}", "2147483648"}) {
                assertThrows(ConfigValidationException.class,
                        () -> parse("QUEUE", type, ",\"maxInflightPerPrefillWorker\":" + value), value);
            }
            FlexlbConfig config = parse("QUEUE", type, "");
            config.getDispatcher().setMaxInflightPerPrefillWorker(0);
            assertThrows(ConfigValidationException.class, () -> FlexlbConfigValidator.validate(config));
        }
    }

    @Test
    void explicit_batch_dispatch_is_rejected_at_startup() {
        var error = assertThrows(ConfigValidationException.class, () -> new ConfigService("""
                {"schemaVersion":3,"requestLifecycle":{"request":{"timeoutMs":60000}},
                 "dispatcher":{"type":"BATCH"}}
                """));
        assertTrue(error.getMessage().contains("dispatcher.type"), error.getMessage());
        assertTrue(error.getMessage().contains("BATCH is no longer supported"), error.getMessage());
    }

    @Test
    void v2_config_ignores_removed_scheduling_fields() {
        FlexlbConfig config = ConfigTestFixtures.parse("""
                {
                  "scheduler":{"type":"QUEUE","ordering":{"type":"FIFO"},
                    "decision":{"type":"FIXED_WINDOW","maxRequests":32},"scanBudgetMultiplier":3},
                  "dispatcher":{"type":"NON_BATCH_V2","maxInflightPerPrefillWorker":5,"fetchAttachTimeoutMs":3000,
                    "maxInflightBatchesPerPrefillWorker":10,"prefillInflightLimitAtDelivery":false}
                }
                """);
        assertEquals(DispatcherConfig.Type.NON_BATCH_V2, config.getDispatcher().getType());
        assertEquals(5, config.getDispatcher().getMaxInflightPerPrefillWorker());
        assertEquals(QueueOrderingConfig.Type.FIFO, config.getScheduler().getOrdering().getType());
    }

    @Test
    void dispatch_starvation_threshold_defaults_to_500ms_and_rejects_negative() {
        assertEquals(500L, parse("QUEUE", "NON_BATCH_V2", "").getDispatcher().getDispatchStarvationThresholdMs());
        FlexlbConfig config = parse("QUEUE", "NON_BATCH_V2", ",\"dispatchStarvationThresholdMs\":5000");
        assertEquals(5000L, config.getDispatcher().getDispatchStarvationThresholdMs());
        assertThrows(ConfigValidationException.class,
                () -> parse("QUEUE", "NON_BATCH_V2", ",\"dispatchStarvationThresholdMs\":-1"));
    }

    @Test
    void cache_match_result_ttl_defaults_to_30s_and_requires_positive_value() {
        assertEquals(30_000L, parse("QUEUE", "NON_BATCH_V2", "").getDispatcher().getCacheMatchResultTtlMs());
        assertEquals(500L, parse("QUEUE", "NON_BATCH_V2", ",\"cacheMatchResultTtlMs\":500").getDispatcher()
                .getCacheMatchResultTtlMs());
        for (String value : new String[]{"0", "-1"}) {
            assertThrows(ConfigValidationException.class,
                    () -> parse("QUEUE", "NON_BATCH_V2", ",\"cacheMatchResultTtlMs\":" + value));
        }
    }

    private static FlexlbConfig parse(String scheduler, String dispatcher, String fields) {
        return ConfigTestFixtures.parse("""
                {"scheduler":{"type":"%s"},"dispatcher":{"type":"%s"%s}}
                """.formatted(scheduler, dispatcher, fields));
    }
}
