package com.retailsvc.vertx.spi.cluster.redis;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.healthchecks.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ITClusterHealthCheck {

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private Vertx vertx;

  @BeforeEach
  void beforeEach() {
    RedisClusterManager clusterManager = RedisClusterManagerTestFactory.newInstance(redis);
    Vertx.builder().withClusterManager(clusterManager).buildClustered(ar -> vertx = ar.result());
    await().until(() -> vertx != null);
  }

  @Test
  void checkHealthOK() {
    Handler<Promise<Status>> handler = ClusterHealthCheck.createProcedure(vertx);
    Promise<Status> promise = Promise.promise();
    handler.handle(promise);
    Status status =
        assertDoesNotThrow(() -> promise.future().toCompletionStage().toCompletableFuture().get());
    assertTrue(status.isOk());
  }

  @Test
  void checkHealthNotOK() {
    redis.stop();
    Handler<Promise<Status>> handler = ClusterHealthCheck.createProcedure(vertx);
    Promise<Status> promise = Promise.promise();
    handler.handle(promise);
    Status status =
        assertDoesNotThrow(() -> promise.future().toCompletionStage().toCompletableFuture().get());
    assertFalse(status.isOk());
  }
}
