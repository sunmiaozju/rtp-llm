package org.flexlb.balance.endpoint;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EndpointGenerationLifecycleTest {

    @Test
    void retirementWaitsForCleanup() throws Exception {
        EndpointGenerationLifecycle lifecycle = new EndpointGenerationLifecycle();
        lifecycle.beginRetirement();
        assertTrue(lifecycle.tryClaimCleanup());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch waiterStarted = new CountDownLatch(1);
            Future<?> concurrentClose = executor.submit(() -> {
                waiterStarted.countDown();
                lifecycle.awaitRetirement();
            });

            assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));
            assertFalse(concurrentClose.isDone(), "close must wait for endpoint cleanup");

            lifecycle.beginCleanup();
            assertFalse(concurrentClose.isDone());
            lifecycle.completeRetirement(null);
            concurrentClose.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentNonOwnerCloseWaitsAndObservesTheSameRetirementFailure() throws Exception {
        EndpointGenerationLifecycle lifecycle = new EndpointGenerationLifecycle();
        lifecycle.beginRetirement();
        assertTrue(lifecycle.tryClaimCleanup());
        IllegalStateException failure = new IllegalStateException("retirement failed");

        CountDownLatch waiterStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> concurrentClose = executor.submit(() -> {
                waiterStarted.countDown();
                lifecycle.awaitRetirement();
            });
            assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));
            assertFalse(concurrentClose.isDone(), "a non-owner close must wait for cleanup");

            lifecycle.beginCleanup();
            lifecycle.completeRetirement(failure);
            ExecutionException observed = assertThrows(
                    ExecutionException.class,
                    () -> concurrentClose.get(1, TimeUnit.SECONDS));
            assertSame(failure, observed.getCause());
        } finally {
            executor.shutdownNow();
        }

        IllegalStateException repeatedObservation = assertThrows(
                IllegalStateException.class, lifecycle::awaitRetirement);
        assertSame(failure, repeatedObservation);
    }

}
