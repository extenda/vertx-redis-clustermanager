package com.retailsvc.vertx.spi.cluster.redis;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.ext.healthchecks.Status;
import java.util.Objects;

/**
 * A helper to create Vert.x cluster {@link io.vertx.ext.healthchecks.HealthChecks} procedures.
 *
 * @author sasjo
 */
public interface ClusterHealthCheck {

  /**
   * Creates a ready-to-use Vert.x cluster {@link io.vertx.ext.healthchecks.HealthChecks} procedure.
   *
   * @param vertx the instance of Vert.x, must not be {@code null}
   * @return a Vert.x cluster {@link io.vertx.ext.healthchecks.HealthChecks} procedure
   */
  static Handler<Promise<Status>> createProcedure(Vertx vertx) {
    Objects.requireNonNull(vertx);
    return healthCheckPromise ->
        vertx.executeBlocking(ClusterHealthCheck::getStatus, false).onComplete(healthCheckPromise);
  }

  /**
   * Get the cluster manager status.
   *
   * @return the health status.
   */
  private static Status getStatus() {
    VertxInternal vertxInternal = (VertxInternal) Vertx.currentContext().owner();
    RedisClusterManager clusterManager = (RedisClusterManager) vertxInternal.clusterManager();
    boolean connected = clusterManager.getRedisInstance().map(RedisInstance::ping).orElse(false);
    return new Status().setOk(connected);
  }
}
