package com.retailsvc.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManager;
import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Test to verify that RedisClusterManager doesn't block the event loop thread.
 */
class RedisClusterManagerThreadingTest {

  @Test
  void testGetNodeInfoDoesNotBlockEventLoop() throws Exception {
    // Create a Vert.x instance with a single event loop thread
    VertxOptions options = new VertxOptions().setEventLoopPoolSize(1);
    Vertx vertx = Vertx.vertx(options);

    try {
      // Create a Redis cluster manager
      RedisConfig config = new RedisConfig();
      RedisClusterManager clusterManager = new RedisClusterManager(config);

      // Initialize the cluster manager
      clusterManager.init(vertx, null);

      // Create a latch to wait for completion
      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean completed = new AtomicBoolean(false);

      // Call getNodeInfo from the event loop thread
      vertx.runOnContext(v -> {
        try {
          clusterManager.getNodeInfo();
          // Should not block, even if result is null
          completed.set(true);
          latch.countDown();
        } catch (Exception e) {
          e.printStackTrace();
          latch.countDown();
        }
      });

      // Wait for completion with a timeout
      boolean success = latch.await(5, TimeUnit.SECONDS);

      // Verify that the operation completed successfully
      assertTrue(success, "Operation should complete within timeout");
      assertTrue(completed.get(), "Operation should complete successfully");

    } finally {
      // Clean up
      vertx.close();
    }
  }

  @Test
  void testMemberAddedDoesNotBlockEventLoop() throws Exception {
    // Create a Vert.x instance with a single event loop thread
    VertxOptions options = new VertxOptions().setEventLoopPoolSize(1);
    Vertx vertx = Vertx.vertx(options);

    try {
      // Create a Redis cluster manager
      RedisConfig config = new RedisConfig();
      RedisClusterManager clusterManager = new RedisClusterManager(config);

      // Initialize the cluster manager
      clusterManager.init(vertx, null);

      // Create a latch to wait for completion
      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean completed = new AtomicBoolean(false);

      // Call memberAdded from the event loop thread
      vertx.runOnContext(v -> {
        try {
          clusterManager.memberAdded("test-node-id");
          // Should not block
          completed.set(true);
          latch.countDown();
        } catch (Exception e) {
          e.printStackTrace();
          latch.countDown();
        }
      });

      // Wait for completion with a timeout
      boolean success = latch.await(5, TimeUnit.SECONDS);

      // Verify that the operation completed successfully
      assertTrue(success, "Operation should complete within timeout");
      assertTrue(completed.get(), "Operation should complete successfully");

    } finally {
      // Clean up
      vertx.close();
    }
  }

  @Test
  void testMemberRemovedDoesNotBlockEventLoop() throws Exception {
    // Create a Vert.x instance with a single event loop thread
    VertxOptions options = new VertxOptions().setEventLoopPoolSize(1);
    Vertx vertx = Vertx.vertx(options);

    try {
      // Create a Redis cluster manager
      RedisConfig config = new RedisConfig();
      RedisClusterManager clusterManager = new RedisClusterManager(config);

      // Initialize the cluster manager
      clusterManager.init(vertx, null);

      // Create a latch to wait for completion
      CountDownLatch latch = new CountDownLatch(1);
      AtomicBoolean completed = new AtomicBoolean(false);

      // Call memberRemoved from the event loop thread
      vertx.runOnContext(v -> {
        try {
          clusterManager.memberRemoved("test-node-id");
          // Should not block
          completed.set(true);
          latch.countDown();
        } catch (Exception e) {
          e.printStackTrace();
          latch.countDown();
        }
      });

      // Wait for completion with a timeout
      boolean success = latch.await(5, TimeUnit.SECONDS);

      // Verify that the operation completed successfully
      assertTrue(success, "Operation should complete within timeout");
      assertTrue(completed.get(), "Operation should complete successfully");

    } finally {
      // Clean up
      vertx.close();
    }
  }
}
