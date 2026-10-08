package com.retailsvc.vertx.spi.cluster.redis;

import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import java.util.Optional;

/**
 * Low-level access to distributed data types backed by Redis. Prefer Vertx shared data types over
 * using this API.
 *
 * @author sasjo
 */
public interface RedisInstance extends RedisDataGrid {

  /**
   * Create a Redis instance from the {@link RedisClusterManager}. If Vertx is not using the Redis
   * cluster manager an empty optional will be returned.
   *
   * @param vertx the vertx instance
   * @return an optional with the Redis instance.
   */
  static Optional<RedisInstance> create(Vertx vertx) {
    if (vertx instanceof VertxInternal vertxInternal) {
      ClusterManager clusterManager = vertxInternal.clusterManager();
      if (clusterManager instanceof RedisClusterManager rcm) {
        return rcm.getRedisInstance();
      }
    }
    return Optional.empty();
  }

  /**
   * Ping the Redis instance(s) to determine availability.
   *
   * @return <code>true</code> if ping is successful, otherwise <code>false</code>.
   */
  boolean ping();
}
