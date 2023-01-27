package io.vertx.spi.cluster.redis;

import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;

/**
 * Low-level access to distributed data types backed by Redis. Prefer Vertx shared data types over
 * using this API.
 *
 * @author sasjo
 */
public interface RedisInstance {

  /**
   * Returns the Redis instance from the {@link RedisClusterManager}. If Vertx is not using the
   * Redis cluster manager an empty optional will be returned.
   *
   * @param vertx the vertx instance
   * @return an optional with the Redis instance.
   */
  static Optional<RedisInstance> getInstance(Vertx vertx) {
    if (vertx instanceof VertxInternal) {
      ClusterManager clusterManager = ((VertxInternal) vertx).getClusterManager();
      if (clusterManager instanceof RedisClusterManager) {
        return ((RedisClusterManager) clusterManager).getRedisInstance();
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

  /**
   * Returns an unbounded blocking queue backed by Redis.
   *
   * @param <V> type of value
   * @param name name of the queue
   * @return A blocking queue instance.
   */
  <V> BlockingQueue<V> getBlockingQueue(String name);

  /**
   * Returns an unbounded blocking deque backed by Redis.
   *
   * @param <V> tyep of value
   * @param name name of the deque
   * @return a blocking deque instance.
   */
  <V> BlockingDeque<V> getBlockingDeque(String name);
}
