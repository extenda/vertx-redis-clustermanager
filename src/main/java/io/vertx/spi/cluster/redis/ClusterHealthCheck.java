package io.vertx.spi.cluster.redis;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
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
    return healthCheckPromise -> {
      vertx.executeBlocking(
          promise -> {
            VertxInternal vertxInternal = (VertxInternal) Vertx.currentContext().owner();
            RedisClusterManager clusterManager =
                (RedisClusterManager) vertxInternal.getClusterManager();
            boolean connected =
                clusterManager.getRedisInstance().map(RedisInstance::ping).orElse(false);
            promise.complete(new Status().setOk(connected));
          },
          false,
          healthCheckPromise);
    };
  }
}
